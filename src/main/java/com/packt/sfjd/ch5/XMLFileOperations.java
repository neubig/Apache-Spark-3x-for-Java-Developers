package com.packt.sfjd.ch5;

import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class XMLFileOperations {

    public static void main(String[] args) {
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Create SparkSession
        SparkSession sparkSession = SparkSession
            .builder()
            .master("local")
            .appName("XMLFileOperations")
            .getOrCreate();
        
        // Get absolute path to the current directory
        String currentDir = System.getProperty("user.dir");
        String inputPath = currentDir + "/src/main/resources/breakfast_menu.xml";
        String outputPath = currentDir + "/src/main/resources/newMenu.xml";
        
        try {
            // Set XML options
            HashMap<String, String> params = new HashMap<String, String>();
            params.put("rowTag", "food");
            params.put("failFast", "true");
            
            // Read XML file
            // In Spark 3.x, the package name has changed from com.databricks.spark.xml to org.apache.spark.sql.xml
            Dataset<Row> docDF = sparkSession.read()
                .format("com.databricks.spark.xml")
                .options(params)
                .load(inputPath);
            
            // Print schema and data
            docDF.printSchema();
            docDF.show();
            
            // Write to XML file
            docDF.write().format("com.databricks.spark.xml")
                .option("rootTag", "food")
                .option("rowTag", "food")
                .save(outputPath);
                
            System.out.println("XML file processed successfully");
        } catch (Exception e) {
            System.err.println("Error processing XML file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close resources
            sparkSession.stop();
        }
    }
}