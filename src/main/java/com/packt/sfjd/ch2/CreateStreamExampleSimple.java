package com.packt.sfjd.ch2;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class CreateStreamExampleSimple {

    public static void main(String[] args) throws IOException {
        
        //Creating Streams using user/programmatically specified elements  
         Stream<String> Userstream = Stream.of("Creating","Streams","from","Specific","elements");
         Userstream.forEach(p -> System.out.println(p));
      
         
         //Creating Streams using array of objects
         Stream<String> ArrayStream = Stream.of( new String[]{"Stream","from","an","array","of","objects"} );
         ArrayStream.forEach(p -> System.out.println(p)); 
     
        
         //Creating Streams from an array
         String[] StringArray=new String[]{"We","can","convert","an","array","to","a","Stream","using","Arrays","as","well"};
         Stream<String> StringStream=Arrays.stream(StringArray);
         StringStream.forEach(p -> System.out.println(p));
         
         //Creating Streams from Collection
         List<Double> myCollection = new ArrayList<>();
         for(int i=0; i<10; i++){
             myCollection.add(Math.random());
         }
                //sequential stream
         Stream<Double> sequentialStream = myCollection.stream();
         sequentialStream.forEach(p -> System.out.println(p));
         		
                 //parallel stream
         Stream<Double> parallelStream = myCollection.parallelStream();
         parallelStream.forEach(p -> System.out.println(p));
         
         
         //Stream from Hashmap
         Map<String, Integer> mapData = new HashMap<>();
         mapData.put("This", 1900);
         mapData.put("is", 2000);
         mapData.put("HashMap", 2100);
         
         mapData.entrySet()
                .stream()
                .forEach(p -> System.out.println(p));
         
         mapData.keySet()
                .stream()
                .forEach(p-> System.out.println(p));
         
         //primitive streams 
         IntStream.range(1, 4)
         .forEach(p -> System.out.println(p));
         
         LongStream.rangeClosed(1, 4)
         .forEach(p -> System.out.println(p));
         
         DoubleStream.of(1.0,2.0,3.0,4.0)
         .forEach(p -> System.out.println(p));
         
         //Streams from File
         try {
             Path filePath = Paths.get("/workspace/Apache-Spark-3x-for-Java-Developers/test_file.txt");
             Stream<String> streamOfStrings = Files.lines(filePath);
             streamOfStrings.forEach(p -> System.out.println("File content: " + p));
         } catch (IOException e) {
             System.out.println("Error reading file: " + e.getMessage());
         }
    }
}