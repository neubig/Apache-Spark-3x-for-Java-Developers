package com.packt.sfjd.ch9;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

//{"flightId":"tz302","timestamp":1494423926816,"temperature":21.12,"landed":false}
public class StateFulProcessingExampleUpdated {
    public static void main(String[] args) throws InterruptedException {
        // Create a SparkSession
        SparkSession sparkSession = SparkSession.builder()
            .master("local[*]")
            .appName("Stateful Streaming Example")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .getOrCreate();

        // Create a JavaStreamingContext
        JavaStreamingContext jssc = new JavaStreamingContext(
            new JavaSparkContext(sparkSession.sparkContext()),
            Durations.milliseconds(1000)
        );
        
        // Set checkpoint directory
        jssc.checkpoint("/tmp/spark-checkpoint");

        // Create a socket stream (you'll need to run a socket server separately)
        JavaReceiverInputDStream<String> inStream = jssc.socketTextStream("localhost", 9999);

        // Parse JSON to FlightDetails objects
        JavaDStream<FlightDetails> flightDetailsStream = inStream.map(x -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(x, FlightDetails.class);
        });

        // Create key-value pairs with flightId as key
        JavaPairDStream<String, FlightDetails> flightDetailsPairStream = flightDetailsStream
            .mapToPair(f -> new Tuple2<String, FlightDetails>(f.getFlightId(), f));

        // Define the state mapping function
        Function3<String, Optional<FlightDetails>, State<List<FlightDetails>>, Tuple2<String, Double>> mappingFunc = 
            (flightId, curFlightDetail, state) -> {
                List<FlightDetails> details = state.exists() ? state.get() : new ArrayList<>();

                boolean isLanded = false;

                if (curFlightDetail.isPresent()) {
                    details.add(curFlightDetail.get());
                    if (curFlightDetail.get().isLanded()) {
                        isLanded = true;
                    }
                }
                
                // Calculate average temperature
                Double avgTemp = details.stream()
                    .mapToDouble(f -> f.getTemperature())
                    .average()
                    .orElse(0.0);

                // Update state based on landing status
                if (isLanded) {
                    state.remove();
                } else {
                    state.update(details);
                }
                
                return new Tuple2<String, Double>(flightId, avgTemp);
            };

        // Apply the stateful transformation
        JavaMapWithStateDStream<String, FlightDetails, List<FlightDetails>, Tuple2<String, Double>> streamWithState = 
            flightDetailsPairStream.mapWithState(
                StateSpec.function(mappingFunc).timeout(Durations.minutes(5))
            );
        
        // Print the results
        streamWithState.print();
        
        System.out.println("Starting stateful streaming example...");
        System.out.println("To test, run a socket server: nc -lk 9999");
        System.out.println("Then send JSON data like: {\"flightId\":\"tz302\",\"timestamp\":1494423926816,\"temperature\":21.12,\"landed\":false}");
        System.out.println("Press Ctrl+C to terminate the application.");
        
        // Start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }
}