package com.mycompany.bigdata.datapipeline.mr;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * Created by zhangxiaofeng on 2017/12/6.
 */
public class Canal2KafkaMapper extends Mapper<Text, Text, Text, Text> {
    protected final static Logger logger = LoggerFactory.getLogger(Canal2KafkaMapper.class);
    private static final Properties props = new Properties();
    private SimpleCanalClient clientTest;

    static {
        try {
            props.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("pipeline.properties"));
        } catch (Exception e) {
            logger.error("load pipeline.properties error.", e);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
        String destination = props.getProperty("canal.destination");
        int port = Integer.parseInt(props.getProperty("canal.port"));
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(props.getProperty("canal.host"),
                port), destination, "", "");

        clientTest = new SimpleCanalClient(destination, props.getProperty("kafka.topic"), producer);
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
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.progress();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
