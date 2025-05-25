package com.packt.sfjd.ch8;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DfExampleUpdated {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("DataFrame Example")
                .master("local[*]")
                .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        
        // Create JavaRDD of Employee objects
        JavaRDD<Employee> empRDD = jsc.parallelize(Arrays.asList(new Employee(1, "Foo"), new Employee(2, "Bar")));
        
        // Convert RDD to DataFrame
        Dataset<Row> df = spark.createDataFrame(empRDD, Employee.class);
        
        // Show the DataFrame
        System.out.println("Original DataFrame:");
        df.show();
        
        // Filter the DataFrame
        Dataset<Row> filtered = df.filter("empId > 1");
        
        // Show the filtered DataFrame
        System.out.println("Filtered DataFrame (empId > 1):");
        filtered.show();
        
        // Stop the SparkSession
        spark.stop();
    }
}