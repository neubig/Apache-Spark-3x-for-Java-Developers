package com.packt.sfjd.ch7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class CustomPartitionerExampleUpdated {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CustomPartitionerExample")
                .master("local[*]")
                .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        JavaPairRDD<String, String> pairRdd = jsc.parallelizePairs(
                Arrays.asList(
                    new Tuple2<>("India", "Asia"),
                    new Tuple2<>("Germany", "Europe"),
                    new Tuple2<>("Japan", "Asia"),
                    new Tuple2<>("France", "Europe")
                ), 3);
        
        // Apply custom partitioner
        JavaPairRDD<String, String> customPartitioned = pairRdd.partitionBy(new CustomPartitioner());
        
        System.out.println("Number of partitions: " + customPartitioned.getNumPartitions());
        
        // Map each partition with its index to see the distribution
        JavaRDD<String> mapPartitionsWithIndex = customPartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list = new ArrayList<>();
            
            while(tupleIterator.hasNext()) {
                list.add("Partition number: " + index + ", key: " + tupleIterator.next()._1());
            }
            
            return list.iterator();
        }, true);
        
        System.out.println("Partition distribution: " + mapPartitionsWithIndex.collect());
        
        // Stop the SparkSession
        spark.stop();
    }
}