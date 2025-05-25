package com.packt.sfjd.ch5;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CSVFileOperations {

    public static void main(String[] args) {
        // Set up Spark session
        SparkSession spark = SparkSession
            .builder()
            .appName("CSVFileOperations")
            .master("local[*]")
            .getOrCreate();
            
        // Set log level
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Read CSV file with inferred schema
        Dataset<Row> csv_read = spark.read()
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/workspace/Apache-Spark-3x-for-Java-Developers/src/main/resources/movies.csv");
            
        System.out.println("CSV with inferred schema:");
        csv_read.printSchema();
        csv_read.show(5);
        
        // Read CSV file with custom schema
        StructType customSchema = new StructType(new StructField[] {
            new StructField("movieId", DataTypes.LongType, true, Metadata.empty()),
            new StructField("title", DataTypes.StringType, true, Metadata.empty()),
            new StructField("genres", DataTypes.StringType, true, Metadata.empty())
        });
        
        Dataset<Row> csv_custom_read = spark.read()
            .format("csv")
            .option("header", "true")
            .schema(customSchema)
            .load("/workspace/Apache-Spark-3x-for-Java-Developers/src/main/resources/movies.csv");
            
        System.out.println("CSV with custom schema:");
        csv_custom_read.printSchema();
        csv_custom_read.show(5);
        
        // Write CSV file
        csv_custom_read.write()
            .format("csv")
            .option("header", "true")
            .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
            .mode("overwrite")
            .save("/tmp/newMovies.csv");
            
        System.out.println("CSV file written to /tmp/newMovies.csv");
        
        // Stop Spark session
        spark.stop();
    }
}