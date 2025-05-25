package com.packt.sfjd.ch9;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FileStreamingExUpdated {
    
    public static void main(String[] args) {
        // Set up the streaming context
        SparkConf conf = new SparkConf().setAppName("FileStreamingExample").setMaster("local[*]");
        String inputDirectory = "/tmp/streamFolder/";
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(1));
        
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN); 
        
        // Create the input directory if it doesn't exist
        try {
            java.nio.file.Files.createDirectories(java.nio.file.Paths.get(inputDirectory));
        } catch (Exception e) {
            System.err.println("Error creating directory: " + e.getMessage());
        }
        
        // Stream text files from the directory
        JavaDStream<String> streamfile = streamingContext.textFileStream(inputDirectory);
        streamfile.print();
        streamfile.foreachRDD(rdd -> rdd.foreach(x -> System.out.println(x)));
        
        // Stream files using Hadoop's TextInputFormat
        JavaPairDStream<LongWritable, Text> streamedFile = streamingContext.fileStream(
            inputDirectory, LongWritable.class, Text.class, TextInputFormat.class);
        streamedFile.print();
        
        // Start the streaming context
        streamingContext.start();
        
        System.out.println("Streaming context started. Waiting for files in " + inputDirectory);
        System.out.println("You can add files to this directory in another terminal to see them processed.");
        System.out.println("Press Ctrl+C to terminate the application.");

        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}