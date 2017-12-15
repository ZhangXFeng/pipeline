package com.zbj.finance.datapipeline.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangxiaofeng on 2017/12/13.
 */
public class KafkaInputFormat extends InputFormat<Text, Text> {
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        List<InputSplit> inputSplits = new ArrayList<>();
        Configuration conf = jobContext.getConfiguration();
        int mapNum = conf.getInt("mapper.number", 1);
        for (int i = 0; i < mapNum; i++) {
            KafkaInputSplit inputSplit = new KafkaInputSplit();
            inputSplits.add(inputSplit);
        }
        return inputSplits;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FakeRecordReader();
    }
}
