package com.packt.sfjd.ch7;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

public class TestAccumulatorUpdated {

    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("TestAccumulatorUpdated")
                .master("local[*]")
                .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        
        // Uncomment to reduce log verbosity
        // Logger rootLogger = LogManager.getRootLogger();
        // rootLogger.setLevel(Level.WARN);
        
        // Create a LongAccumulator to count exceptions
        LongAccumulator longAccumulator = sparkContext.sc().longAccumulator("ExceptionCounter");
        
        // Read the log file
        JavaRDD<String> textFile = sparkContext.textFile("src/main/resources/logFileWithException.log");
        
        // Count exceptions using LongAccumulator
        textFile.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                if(line.contains("Exception")) {
                    longAccumulator.add(1);
                    System.out.println("The intermediate value in loop " + longAccumulator.value());
                }
            }
        });
        
        System.out.println("The final value of LongAccumulator: " + longAccumulator.value());
        
        // Create a CollectionAccumulator to collect exception occurrences
        CollectionAccumulator<Long> collectionAccumulator = sparkContext.sc().collectionAccumulator();
        
        // Count exceptions using CollectionAccumulator
        textFile.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                if(line.contains("Exception")) {
                    collectionAccumulator.add(1L);
                    System.out.println("The intermediate value in loop " + collectionAccumulator.value());
                }
            }
        });
        
        System.out.println("The final value of CollectionAccumulator: " + collectionAccumulator.value());
        
        // Create and register a custom ListAccumulator
        ListAccumulator listAccumulator = new ListAccumulator();
        sparkContext.sc().register(listAccumulator, "ListAccumulator");
        
        // Count exceptions using custom ListAccumulator
        textFile.foreach(new VoidFunction<String>() {
            @Override
            public void call(String line) throws Exception {
                if(line.contains("Exception")) {
                    listAccumulator.add("1");
                    System.out.println("The intermediate value in loop " + listAccumulator.value());
                }
            }
        });
        
        System.out.println("The final value of ListAccumulator: " + listAccumulator.value());
        
        // Stop the SparkSession
        spark.stop();
    }
}