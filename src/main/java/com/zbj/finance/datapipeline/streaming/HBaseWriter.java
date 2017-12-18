package com.zbj.finance.datapipeline.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zhangxiaofeng on 2017/12/7.
 */
public class HBaseWriter extends Writer {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseWriter.class);
    private LinkedBlockingQueue<String> records = new LinkedBlockingQueue<>(10000);
    private static byte[] columnFamily = Bytes.toBytes("cf1");
    private Configuration conf;
    private Processor processor;
    public static final Properties PROP = new Properties();

    static {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource("pipeline.properties");
            PROP.load(url.openStream());
        } catch (Throwable e) {
            LOG.error("load pipeline.properties error.", e);
            System.exit(1);
        }
    }

    public HBaseWriter() {
        init();
    }

    @Override
    public void addRecord(String record) {
        boolean isSuccess = false;
        while (!isSuccess) {
            try {
                records.put(record);
                isSuccess = true;
            } catch (Exception e) {
                LOG.error("put fail. " + record, e);
            }
        }
    }

    public void init() {
        conf = HBaseConfiguration.create();
        try {
            String tableName = PROP.getProperty("hbase.tablename");
            Connection conn = ConnectionFactory.createConnection(conf);
            processor = new Processor(records, conn, TableName.valueOf(tableName));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void start() {
        processor.start();
    }

    private static String getRowKey(String record) {
        String[] a = record.split("\t", -1);
        String[] b = a[0].split(",", -1);
        String database = b[0].split("=", -1)[1];
        String tableName = b[1].split("=", -1)[1];
        StringBuilder rowKeyBuilder = new StringBuilder();
        rowKeyBuilder.append(database + "|").append(tableName);
        for (int i = 1; i < a.length; i++) {
            String[] c = a[i].split(",", -1);
            if (c[3].split("=")[1].equalsIgnoreCase("true")) {
                rowKeyBuilder.append("|" + c[0].split("=", -1)[1]);
            }
        }
        return rowKeyBuilder.toString();
    }

    private static String getEventType(String record) {
        String eventType = null;
        if (record.contains("eventType=INSERT")) {
            eventType = "INSERT";
        } else if (record.contains("eventType=UPDATE")) {
            eventType = "UPDATE";
        } else if (record.contains("eventType=DELETE")) {
            eventType = "DELETE";
        } else {
            LOG.warn("invalid eventType. " + record);
        }
        return eventType;
    }

    /**
     * database=ottertest,table=test,eventType=INSERT	id=28,type=int(11),update=true,isKey=true	name=王子,type=varchar(255),update=true,isKey=false	password=1234,type=varchar(255),update=true,isKey=false	email=sadqw,type=varchar(255),update=true,isKey=false	phone=1213,type=varchar(255),update=true,isKey=false	time=2017-12-07 10:45:45,type=datetime,update=true,isKey=false
     *
     * @param rowKey
     * @param record
     * @return
     */
    private static Mutation str2Mutation(String rowKey, String record) {
        Mutation mutation = null;
        String[] a = record.split("\t", -1);
        if (a[0].contains("UPDATE")) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i = 1; i < a.length; i++) {
                String[] b = a[i].split(",", -1);
                if (b[2].contains("true")) {
                    String[] c = b[0].split("=", -1);
                    String columnName = c[0];
                    String value = c[1];
                    put.addColumn(columnFamily, Bytes.toBytes(columnName), Bytes.toBytes(value));
                }

            }
            mutation = put;
        } else if (a[0].contains("INSERT")) {
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i = 1; i < a.length; i++) {
                String[] b = a[i].split(",", -1);
                String[] c = b[0].split("=", -1);
                String columnName = c[0];
                String value = c[1];
                put.addColumn(columnFamily, Bytes.toBytes(columnName), Bytes.toBytes(value));

            }
            mutation = put;
        } else if (a[0].contains("DELETE")) {
            mutation = new Delete(Bytes.toBytes(rowKey));
        } else {
            //Nothing to do
        }

        return mutation;
    }

    class Processor extends Thread {
        private LinkedBlockingQueue<String> records;
        private TableName tableName;
        private Connection conn;

        Processor(LinkedBlockingQueue<String> records, Connection conn, TableName tableName) {
            this.records = records;
            this.tableName = tableName;
            this.conn = conn;
            this.setName("hbase-processor");
            this.setDaemon(true);
        }


        @Override
        public void run() {
            LOG.info("hbase-processor start.");
            try {
                final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
                    @Override
                    public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                        for (int i = 0; i < e.getNumExceptions(); i++) {
                            LOG.info("Failed to sent put " + e.getRow(i) + ".");
                        }
                    }
                };
                BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                        .listener(listener);
                BufferedMutator mutator = conn.getBufferedMutator(params);
                List<Mutation> mutations = new ArrayList<>();
                long lastPutTime = System.currentTimeMillis();
                while (true) {
                    String record = records.poll();
                    String eventType = null;
                    if (record != null) {
                        String rowKey = getRowKey(record);
                        Mutation mutation = str2Mutation(rowKey, record);
                        if (mutation == null) {
                            continue;
                        }
                        eventType = getEventType(record);
                        mutations.add(mutation);
                    }
                    if (mutations.size() == 1000 || System.currentTimeMillis() - lastPutTime > 2000 || "DELETE".equalsIgnoreCase(eventType) || "UPDATE".equalsIgnoreCase(eventType)) {
                        mutator.mutate(mutations);
                        mutator.flush();
                        lastPutTime = System.currentTimeMillis();
                        mutations = new ArrayList<>();
                    }

                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            } finally {
                LOG.info("hbase-processor down.");
                System.exit(1);
            }
        }
    }
}
