package com.packt.sfjd.ch8;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class ContextCreation {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Sql")
                .master("local[*]")
                .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        
        // In Spark 3.x, SQLContext is deprecated in favor of SparkSession
        // SparkSession provides a unified entry point for working with Spark DataFrames and Datasets
        
        // For Hive support, you can enable it in SparkSession:
        // SparkSession sparkWithHive = SparkSession.builder()
        //         .appName("SqlWithHive")
        //         .master("local[*]")
        //         .enableHiveSupport()
        //         .getOrCreate();
        
        // Stop the SparkSession when done
        spark.stop();
    }
}
