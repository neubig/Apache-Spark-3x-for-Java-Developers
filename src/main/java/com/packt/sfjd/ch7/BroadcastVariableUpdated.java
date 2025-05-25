package com.packt.sfjd.ch7;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class BroadcastVariableUpdated {

	public static void main(String[] args) {
		// Create SparkSession
		SparkSession spark = SparkSession.builder()
				.appName("BroadcastVariable")
				.master("local[*]")
				.getOrCreate();
		
		// Get JavaSparkContext from SparkSession
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		
		// Create a broadcast variable using JavaSparkContext
		Broadcast<String> broadcastVar1 = jsc.broadcast("Hello Spark from JavaSparkContext");
		System.out.println("Broadcast value from JavaSparkContext: " + broadcastVar1.getValue());
		
		// Create a broadcast variable using SparkSession
		Broadcast<String> broadcastVar2 = spark.sparkContext().broadcast(
				"Hello Spark from SparkSession", 
				scala.reflect.ClassTag$.MODULE$.apply(String.class));
		System.out.println("Broadcast value from SparkSession: " + broadcastVar2.getValue());
		
		// Unpersist and destroy broadcast variables
		broadcastVar1.unpersist();
		broadcastVar2.unpersist();
		
		broadcastVar1.destroy();
		broadcastVar2.destroy();
		
		// Stop the SparkSession
		spark.stop();
	}
}