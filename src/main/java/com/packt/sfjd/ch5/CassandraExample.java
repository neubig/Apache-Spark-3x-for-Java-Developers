package com.packt.sfjd.ch5;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

public class CassandraExample {
    public static void main(String[] args) {
        // Set up Spark configuration
        SparkConf conf = new SparkConf()
            .setMaster("local")
            .setAppName("Cassandra Example");
        conf.set("spark.cassandra.connection.host", "127.0.0.1");
        
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
            .config(conf)
            .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        
        // Read from Cassandra table
        JavaRDD<Employee> cassandraTable = CassandraJavaUtil.javaFunctions(jsc)
            .cassandraTable("my_keyspace", "emp", CassandraJavaUtil.mapRowTo(Employee.class));
        
        // Select specific columns
        JavaRDD<String> selectEmpDept = CassandraJavaUtil.javaFunctions(jsc)
            .cassandraTable("my_keyspace", "emp", CassandraJavaUtil.mapColumnTo(String.class))
            .select("emp_dept", "emp_name");
        
        // Print results
        cassandraTable.collect().forEach(System.out::println);
        //selectEmpDept.collect().forEach(System.out::println);
        
        // Write to Cassandra
        CassandraJavaUtil.javaFunctions(cassandraTable)
            .writerBuilder("my_keyspace", "emp1", CassandraJavaUtil.mapToRow(Employee.class))
            .saveToCassandra();
        
        // Using SparkSession to read from Cassandra (SQL API)
        /*
        Map<String, String> options = new HashMap<>();
        options.put("table", "emp");
        options.put("keyspace", "my_keyspace");
        
        Dataset<Row> df = spark.read()
            .format("org.apache.spark.sql.cassandra")
            .options(options)
            .load();
        
        df.show();
        */
        
        // Close resources
        spark.stop();
    }
}