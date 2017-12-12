package com.zbj.finance.datapipeline.streaming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangxiaofeng on 2017/12/12.
 */
public class WriterHolder {
    private static final Logger LOG = LoggerFactory.getLogger(WriterHolder.class);
    private static Writer writer;

    static {
        try {
            writer = new HBaseWriter();
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
        }
    }

    public static Writer getWriter() {
        return writer;
    }
}
