package com.zbj.finance.datapipeline.mr;

import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zhangxiaofeng on 2017/12/8.
 */
public class HiveWriter {
    private static final Logger LOG = LoggerFactory.getLogger(HiveWriter.class);
    private LinkedBlockingQueue<String> records = new LinkedBlockingQueue<>(10000);

    public static final Properties PROP = new Properties();
    public static String metastoreUrl;
    public static String database;
    public static String tablename;
    public static String[] columnNames;
    public static String[] primaryKeys;
    public static String DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static String serverUrl;
    public static String username;
    public static String password;
    public static String DELIMITER = "-=-";
    private Processor processor;

    static {
        try {
            URL url = Thread.currentThread().getContextClassLoader().getResource("pipeline.properties");
            PROP.load(url.openStream());
            metastoreUrl = PROP.getProperty("hive.metastore.url");
            database = PROP.getProperty("hive.database.name");
            tablename = PROP.getProperty("hive.tablename");
            columnNames = PROP.getProperty("hive.table.columnNames").split(",", -1);
            primaryKeys = PROP.getProperty("hive.table.primaryKeys").split(",", -1);
            serverUrl = PROP.getProperty("hive.server.url");
            username = PROP.getProperty("hive.username");
            password = PROP.getProperty("hive.password");
            Class.forName(DRIVER);
        } catch (Throwable e) {
            System.out.println("shit shit shit");
            LOG.error("load pipeline.properties error.", e);
            System.exit(1);
        }
    }

    public HiveWriter() {
        this.processor = new Processor();
    }

    public int getCachedSize() {
        return records.size();
    }

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

    public void start() {
        this.processor.start();
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
     * database=ottertest,table=test,eventType=UPDATE	id=47,type=int(11),update=false,isKey=true	name=snow,type=varchar(255),update=false,isKey=false	password=Bigdata_2013,type=varchar(255),update=false,isKey=false	email=a@b.com,type=varchar(255),update=false,isKey=false	phone=15618378988,type=varchar(255),update=true,isKey=false	time=2017-12-08 13:47:06,type=datetime,update=false,isKey=false
     *
     * @param raw
     * @return
     */
    private static String genDelimitedRecord(String raw) {
        StringBuilder delimitedRecord = new StringBuilder();
        Map<String, String> map = raw2Map(raw);
        for (int i = 0; i < columnNames.length - 1; i++) {
            delimitedRecord.append(map.get(columnNames[i])).append(DELIMITER);
        }
        delimitedRecord.append(map.get(columnNames[columnNames.length - 1]));

        return delimitedRecord.toString();
    }

    private static Map raw2Map(String raw) {
        String[] a = raw.split("\t", -1);
        Map<String, String> map = new HashMap<>();
        for (int i = 1; i < a.length; i++) {
            String[] b = a[i].split(",", -1);
            String[] c = b[0].split("=", -1);
            map.put(c[0], c[1]);
        }
        return map;
    }

    private static Map getUpdateKeyAndValue(String raw) {
        String[] a = raw.split("\t", -1);
        Map<String, String> map = new HashMap<>();
        for (int i = 1; i < a.length; i++) {
            if (a[i].contains("update=true")) {
                String[] b = a[i].split(",", -1);
                String[] c = b[0].split("=", -1);
                map.put(c[0], c[1]);
            }
        }
        return map;
    }

    private static String genUpdateSql(String raw) {
        StringBuilder updateSql = new StringBuilder();
        Map<String, String> updateKeyAndValue = getUpdateKeyAndValue(raw);
        updateSql.append("update " + tablename + " set ");
        Object[] kvs = updateKeyAndValue.entrySet().toArray();
        for (int i = 0; i < kvs.length - 1; i++) {
            Map.Entry<String, String> kv = (Map.Entry<String, String>) kvs[i];
            updateSql.append(kv.getKey() + "='" + kv.getValue() + "', ");
        }
        Map.Entry<String, String> kv = (Map.Entry<String, String>) kvs[kvs.length - 1];
        updateSql.append(kv.getKey() + "='" + kv.getValue() + "'");
        updateSql.append(" where ");
        Map map = raw2Map(raw);
        for (int i = 0; i < primaryKeys.length - 1; i++) {
            updateSql.append(primaryKeys[i] + " = '" + map.get(primaryKeys[i]) + "' and ");
        }
        updateSql.append(primaryKeys[primaryKeys.length - 1] + " = '" + map.get(primaryKeys[primaryKeys.length - 1]) + "'");

        return updateSql.toString();
    }

    private static String genDeleteSql(String raw) {
        StringBuilder deleteSql = new StringBuilder();
        deleteSql.append("delete from ").append(tablename).append(" where ");
        Map map = raw2Map(raw);
        for (int i = 0; i < primaryKeys.length - 1; i++) {
            deleteSql.append(primaryKeys[i] + " = '" + map.get(primaryKeys[i]) + "' and ");
        }
        deleteSql.append(primaryKeys[primaryKeys.length - 1] + " = '" + map.get(primaryKeys[primaryKeys.length - 1]) + "'");
        return deleteSql.toString();
    }

    private static boolean isValid(String record) {
        String[] a = record.split("\t", -1);
        String[] b = a[0].split(",", -1);
        String dbName = b[0].split("=", -1)[1];
        String table = b[1].split("=", -1)[1];
        if (dbName.equals(database) && table.equals(tablename)) {
            return true;
        }
        return false;
    }

    public boolean isWriting() {
        return processor.isWriting();
    }

    class Processor extends Thread {

        private boolean isWriting = false;

        Processor() {
            this.setDaemon(true);
            this.setName("hive-processor");
        }

        public boolean isWriting() {
            return isWriting;
        }

        @Override
        public void run() {
            System.out.println(this.getName() + " start.");
            StreamingConnection conn = null;
            Connection sqlconn = null;
            try {
                HiveEndPoint hep = new HiveEndPoint(metastoreUrl, database, tablename, null);
                conn = hep.newConnection(true);
                DelimitedInputWriter inputWriter = new DelimitedInputWriter(columnNames, DELIMITER, hep);
                sqlconn = DriverManager.getConnection(serverUrl, username, password);

                while (true) {
                    String record = records.poll();
                    if (null != record && isValid(record)) {
                        isWriting = true;
                        String eventType = getEventType(record);
                        System.out.println("Eventtype = " + eventType);
                        if (eventType.equalsIgnoreCase("INSERT")) {
                            TransactionBatch tb = conn.fetchTransactionBatch(10, inputWriter);
                            tb.beginNextTransaction();
                            String delimitedRecord = genDelimitedRecord(record);
                            System.out.println("insert record : " + delimitedRecord);
                            tb.write(delimitedRecord.getBytes());
                            tb.commit();
                            tb.close();
                        } else if (eventType.equalsIgnoreCase("UPDATE")) {
                            Statement stmt = sqlconn.createStatement();
                            String updateSql = genUpdateSql(record);
                            System.out.println("update sql : " + updateSql);
                            stmt.executeUpdate(updateSql);
                            stmt.close();
                        } else if (eventType.equalsIgnoreCase("DELETE")) {
                            Statement stmt = sqlconn.createStatement();
                            String deleteSql = genDeleteSql(record);
                            System.out.println("delete sql : " + deleteSql);
                            stmt.executeUpdate(deleteSql);
                            stmt.close();
                        } else {
                            LOG.warn("invalid eventtype. " + eventType);
                        }
                        isWriting = false;
                    }
                }

            } catch (Throwable e) {

                LOG.error("Processor down. ", e);
                if (conn != null) {
                    conn.close();
                }
                if (sqlconn != null) {
                    try {
                        sqlconn.close();
                    } catch (Exception e2) {
                        LOG.error("sqlconn close fail.", e2);
                    }
                }
            } finally {
                LOG.info("hive-processor down.");
                System.exit(1);
            }
        }
    }
}
