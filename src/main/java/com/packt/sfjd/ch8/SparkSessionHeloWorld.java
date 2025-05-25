package com.packt.sfjd.ch8;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionHelloWorldUpdated {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("CSV Read Example")
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
                .getOrCreate();
        
        // Use relative path to resources
        Dataset<Row> csv = sparkSession.read().format("csv").option("header","true")
                .load("src/main/resources/emp.csv");
        
        csv.createOrReplaceTempView("test");
        Dataset<Row> sql = sparkSession.sql("select * from test");
        sql.show();
    }
}