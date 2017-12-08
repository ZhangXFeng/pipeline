package com.zbj.finance.datapipeline.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangxiaofeng on 2017/12/6.
 */
public class FakeInputFormat extends InputFormat<Text,Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FakeRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        FakeInputSplit inputSplit=new FakeInputSplit();
        List<InputSplit> splits=new ArrayList<InputSplit>();
        splits.add(inputSplit);
        return splits;
    }
}
