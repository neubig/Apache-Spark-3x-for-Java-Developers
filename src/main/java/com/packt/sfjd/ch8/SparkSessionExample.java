package com.packt.sfjd.ch8;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Function1;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.convert.Decorators.AsJava;
import scala.collection.immutable.Map;

public class SparkSessionExample {
	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder()
		.master("local")
		.appName("Spark Session Example")
		// .enableHiveSupport() // Commented out as Hive classes are not available
		.config("spark.driver.memory", "2G")
		.config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
		.getOrCreate();
		
		// sparkSession.conf().set("spark.driver.memory", "3G"); // Cannot modify spark.driver.memory after SparkSession is created
		
		SparkContext sparkContext = sparkSession.sparkContext();
		SparkConf conf = sparkSession.sparkContext().getConf();
		
		Map<String, String> all = sparkSession.conf().getAll();
		 System.out.println(JavaConverters.mapAsJavaMapConverter(all).asJava().get("spark.driver.memory"));
		 
		
		
		
		
		
	}
}
