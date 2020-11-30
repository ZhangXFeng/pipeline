package com.mycompany.bigdata.datapipeline.mr;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhangxiaofeng on 2017/12/13.
 */
public class KafkaRecordReader extends RecordReader<Text, Text> {
    private ConsumerIterator<String, String> it = null;
    private MessageAndMetadata messageAndMetadata = null;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = prop.getProperty("kafka.topic");
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(kafkaParams));
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(1));
        StringDecoder decoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap, decoder, decoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        it = stream.iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        messageAndMetadata = it.next();
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(messageAndMetadata.key().toString());
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(messageAndMetadata.message().toString());
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
