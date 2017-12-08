package com.zbj.finance.datapipeline.straming;

/**
 * Created by zhangxiaofeng on 2017/12/7.
 */
public abstract class Writer {
    private static Writer writer;
    static{
        writer=new HBaseWriter();
    }
    public static Writer getWriter(){
        return writer;
    }
    abstract void addRecord(String record);
    abstract void start();
}
