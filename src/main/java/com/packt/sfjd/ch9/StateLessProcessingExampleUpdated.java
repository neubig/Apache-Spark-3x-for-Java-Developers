package com.packt.sfjd.ch9;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class StateLessProcessingExampleUpdated {
    public static void main(String[] args) throws InterruptedException {
        // Create a SparkSession
        SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("Stateless Streaming Example")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .getOrCreate();

        // Create a JavaStreamingContext
        JavaStreamingContext jssc = new JavaStreamingContext(
            new JavaSparkContext(sparkSession.sparkContext()),
            Durations.milliseconds(1000)
        );

        // Create a socket stream (you'll need to run a socket server separately)
        JavaReceiverInputDStream<String> inStream = jssc.socketTextStream("localhost", 9999);

        // Parse JSON to FlightDetails objects
        JavaDStream<FlightDetails> flightDetailsStream = inStream.map(x -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(x, FlightDetails.class);
        });
        
        // Create a sliding window
        JavaDStream<FlightDetails> window = flightDetailsStream.window(
            Durations.minutes(5),  // window size
            Durations.minutes(1)   // sliding interval
        );
        
        // Transform the window data to calculate average temperature per flight
        JavaPairDStream<String, Double> transformedWindow = window
            // Create key-value pairs with flightId as key and temperature as value
            .mapToPair(f -> new Tuple2<String, Double>(f.getFlightId(), f.getTemperature()))
            // Transform values to (temperature, count) pairs for averaging
            .mapValues(t -> new Tuple2<Double, Integer>(t, 1))
            // Reduce by key to sum temperatures and counts
            .reduceByKey((t1, t2) -> new Tuple2<Double, Integer>(
                t1._1() + t2._1(),  // sum of temperatures
                t1._2() + t2._2()   // sum of counts
            ))
            // Calculate average temperature
            .mapValues(t -> t._1() / t._2());
        
        // Cache the transformed data
        transformedWindow.cache();
        
        // Print the results
        transformedWindow.print();
        
        System.out.println("Starting stateless streaming example...");
        System.out.println("To test, run a socket server: nc -lk 9999");
        System.out.println("Then send JSON data like: {\"flightId\":\"tz302\",\"timestamp\":1494423926816,\"temperature\":21.12,\"landed\":false}");
        System.out.println("Press Ctrl+C to terminate the application.");
        
        // Start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }
}