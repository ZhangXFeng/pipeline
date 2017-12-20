package com.zbj.finance.datapipeline.streaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangxiaofeng on 2017/12/12.
 */
public class WriterHolder {
    private static final Logger LOG = LoggerFactory.getLogger(WriterHolder.class);
    private static Writer[] writers;

    static {
        try {
            int num = Integer.parseInt(StreamingJob.PROP.getProperty("hbase.writer.number", "1"));
            writers=new Writer[num];
            for (int i = 0; i < num; i++) {
                writers[i] = new HBaseWriter();
                writers[i].start();
            }
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    public static Writer getWriter(String key) {
        return writers[key.hashCode() % writers.length];
    }
}
