package com.packt.sfjd.ch11;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * Example demonstrating GraphX functionality in Spark 3.x
 */
public class PropertyGraphExampleUpdated {
    public static void main(String[] args) {
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("GraphXExample")
            .getOrCreate();
            
        try {
            // Define class tags for Scala interoperability
            ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
            ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
            $eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
            
            // Create vertices
            List<Tuple2<Object, String>> vertices = new ArrayList<>();
            vertices.add(new Tuple2<>(1L, "James"));
            vertices.add(new Tuple2<>(2L, "Robert"));
            vertices.add(new Tuple2<>(3L, "Charlie"));
            vertices.add(new Tuple2<>(4L, "Roger"));
            vertices.add(new Tuple2<>(5L, "Tony"));
            
            // Create edges
            List<Edge<String>> edges = new ArrayList<>();
            edges.add(new Edge<>(2L, 1L, "Friend"));
            edges.add(new Edge<>(3L, 2L, "Advisor"));
            edges.add(new Edge<>(3L, 1L, "Friend"));
            edges.add(new Edge<>(4L, 3L, "Colleague"));
            edges.add(new Edge<>(4L, 5L, "Relative"));
            edges.add(new Edge<>(5L, 2L, "BusinessPartners"));
            
            // Create RDDs from vertices and edges
            JavaRDD<Tuple2<Object, String>> verticesRDD = spark.sparkContext().parallelize(
                scala.collection.JavaConverters.asScalaIteratorConverter(vertices.iterator()).asScala().toSeq(),
                2,
                stringTag
            ).toJavaRDD();
            
            JavaRDD<Edge<String>> edgesRDD = spark.sparkContext().parallelize(
                scala.collection.JavaConverters.asScalaIteratorConverter(edges.iterator()).asScala().toSeq(),
                2,
                stringTag
            ).toJavaRDD();
            
            // Create the graph
            Graph<String, String> graph = Graph.apply(
                verticesRDD.rdd(),
                edgesRDD.rdd(),
                "",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                stringTag,
                stringTag
            );
            
            // Display vertices
            System.out.println("Vertices:");
            graph.vertices().toJavaRDD().foreach(v -> System.out.println(v));
            
            // Display edges
            System.out.println("\nEdges:");
            graph.edges().toJavaRDD().foreach(e -> System.out.println(e));
            
            // Display triplets
            System.out.println("\nTriplets:");
            graph.triplets().toJavaRDD().take(5).forEach(System.out::println);
            
            // Demonstrate graph operations
            
            // 1. Map vertices
            System.out.println("\nMap Vertices (adding 'Vertex:' prefix):");
            Graph<String, String> mapVertices = graph.mapVertices(new AbsFunc3(), stringTag, tpEquals);
            mapVertices.vertices().toJavaRDD().foreach(v -> System.out.println(v));
            
            // 2. Map edges (to their string length)
            System.out.println("\nMap Edges (to string length):");
            Graph<String, Integer> mapEdges = graph.mapEdges(new AbsFunc7(), intTag);
            mapEdges.edges().toJavaRDD().foreach(e -> System.out.println(e));
            
            // 3. Map triplets (to edge string length)
            System.out.println("\nMap Triplets (to edge string length):");
            Graph<String, Integer> mapTriplets = graph.mapTriplets(new AbsFunc8(), intTag);
            mapTriplets.triplets().toJavaRDD().take(5).forEach(System.out::println);
            
            // 4. Reverse the graph
            System.out.println("\nReversed Graph Triplets:");
            Graph<String, String> reversedGraph = graph.reverse();
            reversedGraph.triplets().toJavaRDD().take(5).forEach(System.out::println);
            
            // 5. Subgraph (only "Friend" relationships)
            System.out.println("\nSubgraph (only 'Friend' relationships):");
            Graph<String, String> subgraph = graph.subgraph(new AbsFunc1(), new AbsFunc2());
            subgraph.triplets().toJavaRDD().foreach(t -> System.out.println(t));
            
            // 6. Aggregate messages (count incoming edges)
            System.out.println("\nAggregate Messages (count incoming edges):");
            VertexRDD<Integer> aggregateMessages = graph.aggregateMessages(
                new AbsFunc4(),
                new AbsFunc5(),
                TripletFields.All$.MODULE$,
                intTag
            );
            aggregateMessages.toJavaRDD().foreach(v -> System.out.println(v));
            
            // 7. Join vertices with additional data
            System.out.println("\nJoin Vertices with Additional Data:");
            List<Tuple2<Object, String>> dataToJoin = new ArrayList<>();
            dataToJoin.add(new Tuple2<>(1L, "Wilson"));
            dataToJoin.add(new Tuple2<>(2L, "Harmon"));
            dataToJoin.add(new Tuple2<>(3L, "Johnson"));
            dataToJoin.add(new Tuple2<>(4L, "Peterson"));
            dataToJoin.add(new Tuple2<>(5L, "Adams"));
            
            JavaRDD<Tuple2<Object, String>> dataToJoinRdd = spark.sparkContext().parallelize(
                scala.collection.JavaConverters.asScalaIteratorConverter(dataToJoin.iterator()).asScala().toSeq(),
                2,
                stringTag
            ).toJavaRDD();
            
            Graph<String, String> outerJoinVertices = graph.outerJoinVertices(
                dataToJoinRdd.rdd(),
                new AbsFunc6(),
                stringTag,
                stringTag,
                tpEquals
            );
            
            System.out.println("Vertices after join:");
            outerJoinVertices.vertices().toJavaRDD().foreach(v -> System.out.println(v));
            
            // 8. Graph Analytics - Triangle Count
            System.out.println("\nTriangle Count:");
            graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut$.MODULE$);
            Graph<Object, String> triangleCountedGraph = graph.ops().triangleCount();
            triangleCountedGraph.vertices().toJavaRDD().foreach(v -> System.out.println(v));
            
            // 9. Connected Components
            System.out.println("\nConnected Components:");
            Graph<Object, String> connectedComponentsGraph = graph.ops().connectedComponents();
            connectedComponentsGraph.vertices().toJavaRDD().foreach(v -> System.out.println(v));
            
        } catch (Exception e) {
            System.err.println("Error in GraphX example: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Stop Spark session
            spark.stop();
        }
    }
}