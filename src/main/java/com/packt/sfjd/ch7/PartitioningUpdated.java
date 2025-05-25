package com.packt.sfjd.ch7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class PartitioningUpdated {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("PartitioningUpdated")
                .master("local[*]")
                .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Create a pair RDD with 3 partitions
        JavaPairRDD<Integer, String> pairRdd = jsc.parallelizePairs(
                Arrays.asList(
                    new Tuple2<>(1, "A"),
                    new Tuple2<>(2, "B"),
                    new Tuple2<>(3, "C"),
                    new Tuple2<>(4, "D"),
                    new Tuple2<>(5, "E"),
                    new Tuple2<>(6, "F"),
                    new Tuple2<>(7, "G"),
                    new Tuple2<>(8, "H")
                ), 3);
        
        // Convert to Scala RDD for RangePartitioner
        RDD<Tuple2<Integer, String>> rdd = JavaPairRDD.toRDD(pairRdd);
        
        System.out.println("Original number of partitions: " + pairRdd.getNumPartitions());
        
        // Uncomment to use HashPartitioner
        JavaPairRDD<Integer, String> hashPartitioned = pairRdd.partitionBy(new HashPartitioner(2));
        System.out.println("HashPartitioner number of partitions: " + hashPartitioned.getNumPartitions());
        
        // Create a RangePartitioner with 4 partitions
        RangePartitioner rangePartitioner = new RangePartitioner(
                4, rdd, true, 
                scala.math.Ordering.Int$.MODULE$, 
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        
        // Apply the RangePartitioner
        JavaPairRDD<Integer, String> rangePartitioned = pairRdd.partitionBy(rangePartitioner);
        
        // Map each partition with its index to see the distribution
        JavaRDD<String> mapPartitionsWithIndex = rangePartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list = new ArrayList<>();
            
            while(tupleIterator.hasNext()) {
                list.add("Partition number: " + index + ", key: " + tupleIterator.next()._1());
            }
            
            return list.iterator();
        }, true);
        
        System.out.println("RangePartitioner distribution: " + mapPartitionsWithIndex.collect());
        
        // Map each partition with its index for HashPartitioner
        JavaRDD<String> hashPartitionedWithIndex = hashPartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list = new ArrayList<>();
            
            while(tupleIterator.hasNext()) {
                list.add("Partition number: " + index + ", key: " + tupleIterator.next()._1());
            }
            
            return list.iterator();
        }, true);
        
        System.out.println("HashPartitioner distribution: " + hashPartitionedWithIndex.collect());
        
        // Stop the SparkSession
        spark.stop();
    }
}