package com.packt.sfjd.ch7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class CustomPartitionerExample {
public static void main(String[] args) {
	// Create SparkSession
	SparkSession spark = SparkSession.builder()
			.appName("CustomPartitionerExample")
			.master("local[*]")
			.getOrCreate();
	
	// Get JavaSparkContext from SparkSession
	JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	
	 JavaPairRDD<String, String> pairRdd = jsc.parallelizePairs(
				Arrays.asList(new Tuple2<String, String>("India", "Asia"),new Tuple2<String, String>("Germany", "Europe"),
						new Tuple2<String, String>("Japan", "Asia"),new Tuple2<String, String>("France", "Europe"))
						,3);
	 
	 
	 JavaPairRDD<String, String> customPartitioned = pairRdd.partitionBy(new CustomPartitioner());
	 
	 System.out.println(customPartitioned.getNumPartitions());
	 
	 
	 JavaRDD<String> mapPartitionsWithIndex = customPartitioned.mapPartitionsWithIndex((index, tupleIterator) -> {
			
			List<String> list=new ArrayList<>();
			
			while(tupleIterator.hasNext()){
				list.add("Partition number:"+index+",key:"+tupleIterator.next()._1());
			}
			
			return list.iterator();
		}, true);
		
		 System.out.println(mapPartitionsWithIndex.collect());
}
}
