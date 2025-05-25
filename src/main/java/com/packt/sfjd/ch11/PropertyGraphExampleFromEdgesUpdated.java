package com.packt.sfjd.ch11;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.reflect.ClassTag;

/**
 * Example demonstrating creating a GraphX graph from edges in Spark 3.x
 */
public class PropertyGraphExampleFromEdgesUpdated {
    public static void main(String[] args) {
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("GraphXFromEdgesExample")
            .getOrCreate();
            
        try {
            // Define class tags for Scala interoperability
            ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
            
            // Create edges
            List<Edge<String>> edges = new ArrayList<>();
            edges.add(new Edge<>(1L, 2L, "Friend"));
            edges.add(new Edge<>(2L, 3L, "Advisor"));
            edges.add(new Edge<>(1L, 3L, "Friend"));
            edges.add(new Edge<>(4L, 3L, "Colleague"));
            edges.add(new Edge<>(4L, 5L, "Relative"));
            edges.add(new Edge<>(2L, 5L, "BusinessPartners"));
            
            // Create RDD from edges
            JavaRDD<Edge<String>> edgeRDD = spark.sparkContext().parallelize(
                scala.collection.JavaConverters.asScalaIteratorConverter(edges.iterator()).asScala().toSeq(),
                2,
                stringTag
            ).toJavaRDD();
            
            // Create the graph from edges
            Graph<String, String> graph = Graph.fromEdges(
                edgeRDD.rdd(),
                "",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                stringTag,
                stringTag
            );
            
            // Display vertices (automatically created from edges)
            System.out.println("Vertices (automatically created from edges):");
            graph.vertices().toJavaRDD().foreach(v -> System.out.println(v));
            
            // Display edges
            System.out.println("\nEdges:");
            graph.edges().toJavaRDD().foreach(e -> System.out.println(e));
            
            // Display triplets
            System.out.println("\nTriplets:");
            graph.triplets().toJavaRDD().foreach(t -> System.out.println(t));
            
            // Demonstrate some basic graph operations
            
            // 1. Count vertices and edges
            long vertexCount = graph.vertices().count();
            long edgeCount = graph.edges().count();
            System.out.println("\nGraph statistics:");
            System.out.println("Vertex count: " + vertexCount);
            System.out.println("Edge count: " + edgeCount);
            
            // 2. Find vertices with the most connections (degree)
            System.out.println("\nVertex degrees (number of connections):");
            graph.degrees().toJavaRDD().foreach(v -> System.out.println(v));
            
            // 3. Find vertices with the most incoming connections (in-degree)
            System.out.println("\nVertex in-degrees (number of incoming connections):");
            graph.inDegrees().toJavaRDD().foreach(v -> System.out.println(v));
            
            // 4. Find vertices with the most outgoing connections (out-degree)
            System.out.println("\nVertex out-degrees (number of outgoing connections):");
            graph.outDegrees().toJavaRDD().foreach(v -> System.out.println(v));
            
        } catch (Exception e) {
            System.err.println("Error in GraphX from edges example: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Stop Spark session
            spark.stop();
        }
    }
}