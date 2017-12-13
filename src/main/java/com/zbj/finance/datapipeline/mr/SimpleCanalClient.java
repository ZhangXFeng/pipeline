package com.zbj.finance.datapipeline.mr;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleCanalClient extends AbstractCanalClient {
    private static final Properties props = new Properties();
    private Producer<String, String> producer;
    private String topic;

    static {
        try {
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("pipeline.properties"));
        } catch (Exception e) {
            logger.error("load pipeline.properties error.", e);
        }
    }


    public SimpleCanalClient(String destination, String topic, Producer producer) {
        super(destination);
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    protected void pushToExternalSystem(String key, String record) {
        ProducerRecord producerRecord = new ProducerRecord(topic, key, record);
        Future<RecordMetadata> future = producer.send(producerRecord);
        try {
            future.get();
        } catch (Exception e) {
            logger.warn("future.get error. " + record, e);
        }
    }

    public static void main(String args[]) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", props.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
        // 根据ip，直接创建链接，无HA的功能
        String destination = props.getProperty("canal.destination");
        int port = Integer.parseInt(props.getProperty("canal.port"));
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(props.getProperty("canal.host"),
                port), destination, "", "");

        final SimpleCanalClient clientTest = new SimpleCanalClient(destination, props.getProperty("kafka.topic"), producer);
        clientTest.setConnector(connector);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop the canal client");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:\n{}", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
        clientTest.thread.join();
    }

}
