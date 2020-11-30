package com.mycompany.bigdata.datapipeline.mr;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by zhangxiaofeng on 2017/12/6.
 */
public class FakeInputSplit extends InputSplit implements Writable {
    @Override
    public long getLength() throws IOException, InterruptedException {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
