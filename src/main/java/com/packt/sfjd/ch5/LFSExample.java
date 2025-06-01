package com.packt.sfjd.ch5;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class LFSExample {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("Local File System Example")
            .master("local")
            .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        // Get absolute path to the current directory
        String currentDir = System.getProperty("user.dir");
        String inputPath = currentDir + "/src/main/resources/test.txt";
        String outputPath = currentDir + "/src/main/resources/output";
        
        // Uncomment and modify these lines if needed
        // jsc.hadoopConfiguration().setLong("dfs.block.size", 20000);
        // jsc.hadoopConfiguration().setLong("fs.local.block.size", 20000);
        
        // Example 1: Read a single file
        JavaRDD<String> localFile = jsc.textFile(inputPath);
        System.out.println("Number of partitions in localFile: " + localFile.getNumPartitions());
        
        // Word count example (commented out)
        /*
        localFile.flatMap(x -> Arrays.asList(x.split(" ")).iterator())
            .mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
            .reduceByKey((x, y) -> x + y)
            .saveAsTextFile(outputPath);
        */
        
        // Example 2: Read multiple files (commented out)
        /*
        JavaRDD<String> localFile1 = jsc.textFile(inputPath + "," + inputPath2);
        System.out.println("Number of partitions in localFile1: " + localFile1.getNumPartitions());
        */
        
        // Example 3: Read files using wildcard
        try {
            JavaRDD<String> localFile2 = jsc.textFile(currentDir + "/src/main/resources/*");
            System.out.println("Number of partitions in localFile2: " + localFile2.getNumPartitions());
        } catch (Exception e) {
            System.err.println("Error reading files with wildcard: " + e.getMessage());
        }
        
        // Example 4: Read whole text files (commented out)
        /*
        JavaPairRDD<String, String> localFileWhole = jsc.wholeTextFiles(inputPath);
        System.out.println(localFileWhole.collect());
        */
        
        // Close resources
        spark.stop();
    }
}