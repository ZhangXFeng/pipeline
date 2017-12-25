package com.zbj.finance.datapipeline.mr;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Created by zhangxiaofeng on 2017/12/13.
 */
public class Kafka2HiveMapper extends Mapper<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(Kafka2HiveMapper.class);
    private Thread reporter;
    private ConsumerIterator<String, String> it;
    private ConsumerConnector consumer;
    private ReentrantLock lock = new ReentrantLock();
    private HiveWriter[] writers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        reporter = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        context.progress();
                        Thread.sleep(60000);
                    }
                } catch (Throwable e) {
                    LOG.error("reporter down. ", e);
                } finally {
                    System.exit(1);
                }
            }
        });
        reporter.setName("reporter");
        reporter.setDaemon(true);
        reporter.start();
        Properties prop = new Properties();
        URL url = Thread.currentThread().getContextClassLoader().getResource("pipeline.properties");
        prop.load(url.openStream());
        Properties kafkaParams = new Properties();
        kafkaParams.put("zookeeper.connect", prop.getProperty("kafka.zookeeper.connect"));
        kafkaParams.put("group.id", prop.getProperty("kafka.hive.group.id"));
        kafkaParams.put("zookeeper.session.timeout.ms", "5000");
        kafkaParams.put("zookeeper.connection.timeout.ms", "100000");
        kafkaParams.put("rebalance.backoff.ms", "2000");
        kafkaParams.put("rebalance.max.retries", "10");
        kafkaParams.put("zookeeper.sync.time.ms", "200");
        kafkaParams.put("auto.commit.enable", "false");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.forEach(new BiConsumer<Object, Object>() {
            @Override
            public void accept(Object key, Object value) {
                if (key.toString().startsWith("kafka.")) {
                    String configName = key.toString().substring(6);
                    kafkaParams.put(configName, value.toString());
                }
            }
        });
        String topic = prop.getProperty("kafka.topic");
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaParams));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));
        StringDecoder decoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, decoder, decoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        it = stream.iterator();

        int num = Integer.parseInt(prop.getProperty("hive.writer.number", "1"));
        writers = new HiveWriter[num];
        for (int i = 0; i < num; i++) {
            writers[i] = new HiveWriter();
            writers[i].start();
            LOG.info("start hiveWriter, index " + i);
        }

        TimerTask commitTask = new TimerTask() {
            @Override
            public void run() {
                lock.lock();
                try {
                    while (true) {
                        if (getCachedSize() == 0 && !isWriting()) {
                            consumer.commitOffsets();
                            break;
                        }
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    LOG.error("sleep interrupted. ", e);
                } finally {
                    lock.unlock();
                }
            }
        };
        Timer commitTimer = new Timer();
        commitTimer.scheduleAtFixedRate(commitTask, 1000, 1000);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        MessageAndMetadata messageAndMetadata = it.next();
        lock.lock();
        writers[messageAndMetadata.key().hashCode() % writers.length].addRecord(messageAndMetadata.message().toString());
        lock.unlock();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }

    private int getCachedSize() {
        int size = 0;
        for (int i = 0; i < writers.length; i++) {
            size += writers[i].getCachedSize();
        }
        return size;
    }

    private boolean isWriting() {
        for (int i = 0; i < writers.length; i++) {
            if (writers[i].isWriting()) {
                return true;
            }
        }
        return false;
    }

}
