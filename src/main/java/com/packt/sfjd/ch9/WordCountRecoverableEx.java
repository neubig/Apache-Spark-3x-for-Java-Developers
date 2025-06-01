package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCountRecoverableExUpdated {

    public static void main(String[] args) throws Exception {
        final String ip = "localhost";
        final int port = 9000;
        final String checkpointDirectory = "/tmp/spark-checkpoint";
        
        // Function to create JavaStreamingContext without any output operations
        // (used to detect the new context)
        Function0<JavaStreamingContext> createContextFunc = new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() {
                return createContext(ip, port, checkpointDirectory);
            }
        };

        // Get or create the streaming context
        JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);
        
        System.out.println("Starting word count recoverable example...");
        System.out.println("To test, run a socket server: nc -lk 9000");
        System.out.println("Then type words to see them counted with state maintained");
        System.out.println("Press Ctrl+C to terminate the application.");
        
        ssc.start();
        ssc.awaitTermination();
    }

    protected static JavaStreamingContext createContext(String ip, int port, String checkpointDirectory) {
        // Create a new streaming context
        SparkConf sparkConf = new SparkConf()
            .setAppName("WordCountRecoverableEx")
            .setMaster("local[*]");
            
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        streamingContext.checkpoint(checkpointDirectory);
        
        // Initial state RDD input to mapWithState
        List<Tuple2<String, Integer>> tuples = Arrays.asList(
            new Tuple2<>("hello", 1), 
            new Tuple2<>("world", 1)
        );
        JavaPairRDD<String, Integer> initialRDD = streamingContext.sparkContext().parallelizePairs(tuples);

        // Create a socket stream
        JavaReceiverInputDStream<String> streamingLines = streamingContext.socketTextStream(
            ip,
            port, 
            StorageLevels.MEMORY_AND_DISK_SER
        );

        // Split input into words
        JavaDStream<String> words = streamingLines.flatMap(
            str -> Arrays.asList(str.split(" ")).iterator()
        );

        // Count words in each batch
        JavaPairDStream<String, Integer> wordCounts = words
            .mapToPair(str -> new Tuple2<>(str, 1))
            .reduceByKey((count1, count2) -> count1 + count2);

        // Update the cumulative count function
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc = 
            (word, one, state) -> {
                int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                state.update(sum);
                return output;
            };

        // DStream made of get cumulative counts that get updated in every batch
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream = 
            wordCounts.mapWithState(
                StateSpec.function(mappingFunc).initialState(initialRDD)
            );

        // Print the results
        stateDstream.print();
        
        return streamingContext;
    }
}