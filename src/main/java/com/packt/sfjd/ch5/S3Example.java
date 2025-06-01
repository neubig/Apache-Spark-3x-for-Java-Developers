package com.packt.sfjd.ch5;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class S3Example {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("S3 Example")
            .master("local")
            .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        // Configure S3 credentials
        // For Spark 3.x, it's recommended to use the s3a:// protocol
        // jsc.hadoopConfiguration().set("fs.s3a.access.key", "Your awsAccessKeyId");
        // jsc.hadoopConfiguration().set("fs.s3a.secret.key", "your awsSecretAccessKey");
        // jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        
        // Print AWS access key from environment (for debugging)
        System.out.println("AWS_ACCESS_KEY_ID: " + System.getenv("AWS_ACCESS_KEY_ID"));
        
        try {
            // Read from S3
            JavaRDD<String> textFile = jsc.textFile("s3a://trust/MOCK_DATA.csv");
            
            // Process and write back to S3
            textFile.flatMap(x -> Arrays.asList(x.split(",")).iterator())
                .mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
                .reduceByKey((x, y) -> x + y)
                .saveAsTextFile("s3a://trust/out.txt");
                
            System.out.println("S3 operation completed successfully");
        } catch (Exception e) {
            System.err.println("Error accessing S3: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close resources
            spark.stop();
        }
    }
}