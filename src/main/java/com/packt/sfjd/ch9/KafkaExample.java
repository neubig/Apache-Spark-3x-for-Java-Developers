package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaExampleUpdated {

    public static void main(String[] args) {
        // Set up the streaming context
        SparkConf conf = new SparkConf().setAppName("KafkaExample").setMaster("local[*]");    
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.minutes(2));
        streamingContext.checkpoint("/tmp/checkpoint");
        
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN); 
        
        // Kafka configuration
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark_streaming_group");
        kafkaParams.put("auto.offset.reset", "latest");
        // kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("mytopic", "anothertopic");

        // Create direct Kafka stream
        final JavaInputDStream<ConsumerRecord<String, String>> stream = 
            KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

        // Process the stream
        JavaPairDStream<String, String> pairRDD = stream.mapToPair(record -> 
            new Tuple2<>(record.key(), record.value())
        );
       
        pairRDD.foreachRDD(pRDD -> {
            pRDD.foreach(tuple -> 
                System.out.println(new Date() + " :: Kafka msg key ::" + tuple._1() + " the val is ::" + tuple._2())
            );
        });
       
        JavaDStream<String> tweetRDD = pairRDD.map(x -> x._2()).map(new TweetText());
        
        tweetRDD.foreachRDD(tRDD -> 
            tRDD.foreach(x -> System.out.println(new Date() + " :: " + x))
        );
        
        JavaDStream<String> hashtagRDD = tweetRDD.flatMap(twt -> 
            Arrays.stream(twt.split(" "))
                .filter(str -> str.contains("#"))
                .collect(Collectors.toList())
                .iterator()
        );
   
        hashtagRDD.foreachRDD(tRDD -> 
            tRDD.foreach(x -> System.out.println(x))
        );
        
        JavaPairDStream<String, Long> cntByVal = hashtagRDD.countByValue();
        
        cntByVal.foreachRDD(tRDD -> 
            tRDD.foreach(x -> System.out.println(new Date() + " ::The count tag is ::" + x._1() + " and the val is ::" + x._2()))
        );
        
        // Window operations
        hashtagRDD.window(Durations.minutes(8)).countByValue()
            .foreachRDD(tRDD -> 
                tRDD.foreach(x -> System.out.println(new Date() + " ::The window count tag is ::" + x._1() + " and the val is ::" + x._2()))
            );
            
        hashtagRDD.window(Durations.minutes(8), Durations.minutes(2)).countByValue()
            .foreachRDD(tRDD -> 
                tRDD.foreach(x -> System.out.println(new Date() + " ::The window count tag is ::" + x._1() + " and the val is ::" + x._2()))
            );
            
        hashtagRDD.window(Durations.minutes(12), Durations.minutes(8)).countByValue()
            .foreachRDD(tRDD -> 
                tRDD.foreach(x -> System.out.println(new Date() + " ::The window count tag is ::" + x._1() + " and the val is ::" + x._2()))
            );
            
        hashtagRDD.window(Durations.minutes(2), Durations.minutes(2)).countByValue()
            .foreachRDD(tRDD -> 
                tRDD.foreach(x -> System.out.println(new Date() + " ::The window count tag is ::" + x._1() + " and the val is ::" + x._2()))
            );
            
        hashtagRDD.window(Durations.minutes(12), Durations.minutes(12)).countByValue()
            .foreachRDD(tRDD -> 
                tRDD.foreach(x -> System.out.println(new Date() + " ::The window count tag is ::" + x._1() + " and the val is ::" + x._2()))
            );
       
        // Start the streaming context
        System.out.println("Starting Kafka streaming example...");
        System.out.println("Note: This example requires a running Kafka broker with the specified topics.");
        System.out.println("Press Ctrl+C to terminate the application.");
        
        streamingContext.start();
        
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}