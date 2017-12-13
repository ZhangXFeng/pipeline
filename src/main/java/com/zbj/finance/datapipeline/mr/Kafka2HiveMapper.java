package com.zbj.finance.datapipeline.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhangxiaofeng on 2017/12/13.
 */
public class Kafka2HiveMapper extends Mapper<Text, Text, Text, Text> {
    private HiveWriter hiveWriter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        hiveWriter = new HiveWriter();
        hiveWriter.start();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        hiveWriter.addRecord(value.toString());
        context.progress();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
