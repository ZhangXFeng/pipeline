package com.mycompany.bigdata.datapipeline.streaming;

import kafka.serializer.StringDecoder;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.net.URL;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * Created by zhangxiaofeng on 2017/12/7.
 */
public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    public static final Properties PROP = new Properties();

    static {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource("pipeline.properties");
            PROP.load(url.openStream());
        } catch (Exception e) {
            LOG.error("load pipeline.properties error.", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        int batchInterval = Integer.parseInt(PROP.getProperty("streaming.batchInterval"));
        final String database = PROP.getProperty("kafka.record.database");
        final String tableName = PROP.getProperty("kafka.record.tablename");
        boolean isDirect = Boolean.parseBoolean(PROP.getProperty("isDirect", "true"));
        final String checkpoint = isDirect ? "pipeline-direct-checkpoint" : "pipeline-checkpoint";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(checkpoint, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                System.out.println("create a new streaming context.");
                SparkConf sparkConf = new SparkConf().set("spark.streaming.receiver.writeAheadLog.enable", "true")
                        .set("spark.streaming.backpressure.enabled", "true")
                        .set("spark.streaming.stopGracefullyOnShutdown", "true")
                        .set("spark.rdd.compress", "true")
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .setAppName("pipeline");
                JavaSparkContext jsc = new JavaSparkContext(sparkConf);
                JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(batchInterval));
                jssc.checkpoint(checkpoint);
                //实时变更流
                JavaPairDStream<String, String> messages = createStream(jssc);
                JavaPairDStream<String, String> values = messages.filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        String[] a = tuple._1.split("\\|", -1);
                        return a[0].equals(database) && a[1].equals(tableName);
                    }
                });
                values.foreachRDD(new VoidFunction2<JavaPairRDD<String, String>, Time>() {
                    @Override
                    public void call(JavaPairRDD<String, String> kvRdd, Time time) throws Exception {
                        kvRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                            @Override
                            public void call(Iterator<Tuple2<String, String>> kvs) throws Exception {
                                while(kvs.hasNext()){
                                    Tuple2 kv=kvs.next();
                                    String key=kv._1.toString();
                                    String value=kv._2.toString();
                                    WriterHolder.getWriter(key).addRecord(value);
                                }
                            }
                        });
                    }
                });
                return jssc;
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }

    private static JavaPairDStream<String, String> createStream(JavaStreamingContext jssc) {
        String topic = PROP.getProperty("kafka.topic");
        String groupId = PROP.getProperty("kafka.hbase.group.id");
        Integer receiverNum = Integer.parseInt(PROP.getProperty("streaming.receiver.num", 1 + ""));
        boolean isDirect = Boolean.parseBoolean(PROP.getProperty("isDirect", "true"));
        JavaPairDStream<String, String> messages;
        final Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", PROP.getProperty("kafka.bootstrap.servers"));
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("enable.auto.commit", "true");
        PROP.forEach(new BiConsumer<Object, Object>() {
            @Override
            public void accept(Object key, Object value) {
                if (key.toString().startsWith("kafka.")) {
                    String configName = key.toString().substring(6);
                    kafkaParams.put(configName, value.toString());
                }
            }
        });
        if (isDirect) {
            Set<String> topics = new HashSet<>();
            topics.add(topic);
            messages = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        } else {
            String zkQuorum = PROP.getProperty("kafka.zookeeper.connect");
            kafkaParams.put("zookeeper.connect", zkQuorum);
            Map<String, Integer> topicMap = new HashMap<>();
            topicMap.put(topic, 2);
            List<JavaPairDStream<String, String>> streams = new ArrayList<>();
            for (int i = 0; i < receiverNum; i++) {
                streams.add(KafkaUtils.createStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER()));
            }
            messages = jssc.union(streams.get(0), streams.subList(1, streams.size()));
        }
        return messages;
    }
}
