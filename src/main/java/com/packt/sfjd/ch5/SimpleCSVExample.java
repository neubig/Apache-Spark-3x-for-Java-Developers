package com.packt.sfjd.ch5;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleCSVExample {

    public static void main(String[] args) {
        // Set up Spark session
        SparkSession spark = SparkSession
            .builder()
            .appName("SimpleCSVExample")
            .master("local[*]")
            .getOrCreate();
            
        // Set log level
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Read CSV file
        Dataset<Row> df = spark.read()
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/workspace/Apache-Spark-3x-for-Java-Developers/src/main/resources/movies.csv");
            
        // Show schema and data
        System.out.println("Schema:");
        df.printSchema();
        
        System.out.println("Data:");
        df.show(5);
        
        // Stop Spark session
        spark.stop();
    }
}