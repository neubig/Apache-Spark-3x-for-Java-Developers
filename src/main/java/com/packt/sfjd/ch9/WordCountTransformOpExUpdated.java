package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public final class WordCountTransformOpExUpdated {
     
    public static void main(String[] args) throws Exception {
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf()
            .setAppName("WordCountTransformOpEx")
            .setMaster("local[*]");
            
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        
        // Create initial RDD for join operation
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
        
        // Split each line into words
        JavaDStream<String> words = streamingLines.flatMap(
            str -> Arrays.asList(str.split(" ")).iterator()
        );
       
        // Count each word in each batch
        JavaPairDStream<String, Integer> wordCounts = words
            .mapToPair(str -> new Tuple2<>(str, 1))
            .reduceByKey((count1, count2) -> count1 + count2);
       
        // Print the word counts
        wordCounts.print();
        
        // Transform operation to join with the initial RDD
        JavaPairDStream<String, Integer> joinedDstream = wordCounts
            .transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                @Override
                public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> rdd) throws Exception {
                    // Join with initialRDD and sum the values
                    JavaPairRDD<String, Integer> modRDD = rdd.join(initialRDD)
                        .mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> joinedTuple) 
                                    throws Exception {
                                return new Tuple2<>(
                                    joinedTuple._1(),
                                    joinedTuple._2()._1() + joinedTuple._2()._2()
                                );
                            }
                        });
                    return modRDD;
                }
            });

        // Print the joined stream
        joinedDstream.print();
        
        System.out.println("Starting word count transform operation example...");
        System.out.println("To test, run a socket server: nc -lk 9000");
        System.out.println("Then type words to see them counted and transformed");
        System.out.println("Press Ctrl+C to terminate the application.");
        
        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}