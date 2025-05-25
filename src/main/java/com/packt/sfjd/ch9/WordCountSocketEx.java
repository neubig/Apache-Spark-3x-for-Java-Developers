package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public final class WordCountSocketExUpdated {

    public static void main(String[] args) throws Exception {
        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf()
            .setAppName("WordCountSocketEx")
            .setMaster("local[*]");
            
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // Create a socket stream
        JavaReceiverInputDStream<String> streamingLines = streamingContext.socketTextStream(
            "localhost",
            9000,
            StorageLevels.MEMORY_AND_DISK_SER
        );

        // Split each line into words
        JavaDStream<String> words = streamingLines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String str) {
                return Arrays.asList(str.split(" ")).iterator();
            }
        });

        // Count each word in each batch
        JavaPairDStream<String, Integer> wordCounts = words
            .mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String str) {
                    return new Tuple2<>(str, 1);
                }
            })
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer count1, Integer count2) {
                    return count1 + count2;
                }
            });

        // Print the results
        wordCounts.print();
        
        System.out.println("Starting word count socket example...");
        System.out.println("To test, run a socket server: nc -lk 9000");
        System.out.println("Then type words to see them counted");
        System.out.println("Press Ctrl+C to terminate the application.");

        // Start the computation
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}