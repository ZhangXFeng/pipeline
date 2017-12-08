package com.zbj.finance.datapipeline.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by zhangxiaofeng on 2017/12/6.
 */
public class FakeRecordReader extends RecordReader<Text,Text> {
    private Text fakeText=new Text("fake");
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return fakeText;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return fakeText;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
