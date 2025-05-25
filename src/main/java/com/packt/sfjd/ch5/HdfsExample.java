package com.packt.sfjd.ch5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class HdfsExample {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("HDFS Example")
            .master("local")
            .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        // Set HDFS block size
        jsc.hadoopConfiguration().setLong("dfs.blocksize", 2);
        
        // Read from HDFS
        JavaRDD<String> hadoopRdd = jsc.textFile("hdfs://ch3lxesgdi02.corp.equinix.com:8020/user/gse/packt/ch01/test1", 2);
        
        // Print number of partitions
        System.out.println(hadoopRdd.getNumPartitions());
        
        // Write to HDFS (commented out)
        //hadoopRdd.saveAsTextFile("hdfs://ch3lxesgdi02.corp.equinix.com:8020/user/gse/packt/ch01/testout");
        
        // Close resources
        spark.stop();
    }
}