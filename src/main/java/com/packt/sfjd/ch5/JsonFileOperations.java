package com.packt.sfjd.ch5;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFileOperations {

    public static void main(String[] args) {
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Create SparkSession
        SparkSession sparkSession = SparkSession
            .builder()
            .master("local")
            .appName("JsonFileOperations")
            .getOrCreate();
        
        // Get absolute path to the current directory
        String currentDir = System.getProperty("user.dir");
        String jsonFilePath = currentDir + "/src/main/resources/pep_json.json";
        String outputPath = currentDir + "/src/main/resources/pep_out.json";
        
        // Read JSON file as RDD
        RDD<String> textFile = sparkSession.sparkContext().textFile(jsonFilePath, 2);
        
        // Parse JSON to Java objects
        JavaRDD<PersonDetails> mapParser = textFile.toJavaRDD()
            .map(v1 -> {
                try {
                    return new ObjectMapper().readValue(v1, PersonDetails.class);
                } catch (Exception e) {
                    System.err.println("Error parsing JSON: " + e.getMessage());
                    return null;
                }
            })
            .filter(obj -> obj != null);
        
        // Print parsed objects
        mapParser.foreach(t -> System.out.println(t));
        
        // Read JSON using Spark SQL
        Dataset<Row> anotherPeople = sparkSession.read().json(textFile);
        
        // Print schema and data
        anotherPeople.printSchema();
        anotherPeople.show();
        
        // Read JSON file directly
        Dataset<Row> json_rec = sparkSession.read().json(jsonFilePath);
        json_rec.printSchema();
        json_rec.show();
        
        // Define schema
        StructType schema = new StructType(new StructField[] {
            DataTypes.createStructField("cid", DataTypes.IntegerType, true),
            DataTypes.createStructField("county", DataTypes.StringType, true),
            DataTypes.createStructField("firstName", DataTypes.StringType, true),
            DataTypes.createStructField("sex", DataTypes.StringType, true),
            DataTypes.createStructField("year", DataTypes.StringType, true),
            DataTypes.createStructField("dateOfBirth", DataTypes.TimestampType, true)
        });
        
        // Read JSON with schema
        Dataset<Row> person_mod = sparkSession.read().schema(schema).json(textFile);
        
        // Print schema and data
        person_mod.printSchema();
        person_mod.show();
        
        // Write to JSON file
        person_mod.write().format("json").mode("overwrite").save(outputPath);
        
        // Close resources
        sparkSession.stop();
    }
}