package com.zbj.finance.datapipeline.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zhangxiaofeng on 2017/12/13.
 */
public class Kafka2HiveMapper extends Mapper<Text, Text, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(Kafka2HiveMapper.class);
    private HiveWriter hiveWriter;
    private Thread reporter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        hiveWriter = new HiveWriter();
        hiveWriter.start();
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
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        hiveWriter.addRecord(value.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
