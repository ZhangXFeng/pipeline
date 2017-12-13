package com.zbj.finance.datapipeline.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by zhangxiaofeng on 2017/12/13.
 */
public class Kafka2Hive {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path confPath = Paths.get(args[0]);
        File[] confFiles = confPath.toFile().listFiles();
        StringBuilder confFilesStr = new StringBuilder();
        for (File file : confFiles) {
            confFilesStr.append("file://" + file.getAbsolutePath() + ",");
        }
        System.out.println("\n\n\n" + confFilesStr.toString() + "\n\n\n");
        conf.set("tmpfiles", confFilesStr.toString());
        Path libPath = Paths.get(args[1]);
        File[] jarFiles = libPath.toFile().listFiles();
        StringBuilder jarsStr = new StringBuilder();
        for (File file : jarFiles) {
            jarsStr.append("file://" + file.getAbsolutePath() + ",");
        }
        System.out.println("\n\n\n" + jarsStr.toString() + "\n\n\n");
        conf.set("tmpjars", jarsStr.toString());

        conf.setInt("mapper.number", Integer.parseInt(args[2]));

        Job job = Job.getInstance(conf, "kafka to hive");
        job.setJarByClass(Kafka2Hive.class);
        job.setMapperClass(Kafka2HiveMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(KafkaInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
