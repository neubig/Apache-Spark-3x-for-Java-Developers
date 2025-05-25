package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WindowBatchIntervalUpdated {
    
    public static void main(String[] args) {
        // Set up the streaming context
        SparkConf conf = new SparkConf()
            .setAppName("WindowBatchIntervalExample")
            .setMaster("local[*]");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.minutes(2));
        streamingContext.checkpoint("/tmp/spark-checkpoint");
        
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Create initial RDD for testing
        List<Tuple2<String, Integer>> tuples = Arrays.asList(
            new Tuple2<>("hello", 10), 
            new Tuple2<>("world", 10)
        );
        JavaPairRDD<String, Integer> initialRDD = streamingContext.sparkContext().parallelizePairs(tuples);
        
        // Create a socket stream
        JavaReceiverInputDStream<String> streamingLines = streamingContext.socketTextStream(
            "localhost",
            9000,
            StorageLevels.MEMORY_AND_DISK_SER
        );
        
        // Split input into words
        JavaDStream<String> words = streamingLines.flatMap(
            str -> Arrays.asList(str.split(" ")).iterator()
        );
        
        // Count words
        JavaPairDStream<String, Integer> wordCounts = words
            .mapToPair(str -> new Tuple2<>(str, 1))
            .reduceByKey((count1, count2) -> count1 + count2);
        
        // Print the word counts
        wordCounts.print();
        
        // Apply various window operations
        
        // Window with only window length
        wordCounts.window(Durations.minutes(8))
            .countByValue()
            .foreachRDD(tRDD -> tRDD.foreach(x -> 
                System.out.println(new Date() + " :: Window(8min) count :: " + 
                    x._1() + " and the val is :: " + x._2())
            ));
        
        // Window with length and sliding interval
        wordCounts.window(Durations.minutes(8), Durations.minutes(2))
            .countByValue()
            .foreachRDD(tRDD -> tRDD.foreach(x -> 
                System.out.println(new Date() + " :: Window(8min,2min) count :: " + 
                    x._1() + " and the val is :: " + x._2())
            ));
        
        wordCounts.window(Durations.minutes(12), Durations.minutes(8))
            .countByValue()
            .foreachRDD(tRDD -> tRDD.foreach(x -> 
                System.out.println(new Date() + " :: Window(12min,8min) count :: " + 
                    x._1() + " and the val is :: " + x._2())
            ));
        
        wordCounts.window(Durations.minutes(2), Durations.minutes(2))
            .countByValue()
            .foreachRDD(tRDD -> tRDD.foreach(x -> 
                System.out.println(new Date() + " :: Window(2min,2min) count :: " + 
                    x._1() + " and the val is :: " + x._2())
            ));
        
        wordCounts.window(Durations.minutes(12), Durations.minutes(12))
            .countByValue()
            .foreachRDD(tRDD -> tRDD.foreach(x -> 
                System.out.println(new Date() + " :: Window(12min,12min) count :: " + 
                    x._1() + " and the val is :: " + x._2())
            ));
        
        // Additional window operations (can be commented out if causing issues)
        wordCounts.window(Durations.minutes(5), Durations.minutes(2))
            .countByValue()
            .foreachRDD(tRDD -> tRDD.foreach(x -> 
                System.out.println(new Date() + " :: Window(5min,2min) count :: " + 
                    x._1() + " and the val is :: " + x._2())
            ));
        
        wordCounts.window(Durations.minutes(10), Durations.minutes(1))
            .countByValue()
            .foreachRDD(tRDD -> tRDD.foreach(x -> 
                System.out.println(new Date() + " :: Window(10min,1min) count :: " + 
                    x._1() + " and the val is :: " + x._2())
            ));
        
        System.out.println("Starting window batch interval example...");
        System.out.println("To test, run a socket server: nc -lk 9000");
        System.out.println("Then type words to see them counted in various windows");
        System.out.println("Press Ctrl+C to terminate the application.");
        
        // Start the streaming context
        streamingContext.start();
        
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}