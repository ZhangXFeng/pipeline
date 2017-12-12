package com.zbj.finance.datapipeline.streaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangxiaofeng on 2017/12/7.
 */
public abstract class Writer {

    abstract void addRecord(String record);

    abstract void start();
}
