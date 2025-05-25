package com.packt.sfjd.ch5;

import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class TextFileOperations {

    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("TextFileOperations")
            .master("local")
            .config("spark.hadoop.validateOutputSpecs", "false")
            .getOrCreate();
        
        // Get JavaSparkContext from SparkSession
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        
        // Get absolute path to the current directory
        String currentDir = System.getProperty("user.dir");
        String inputPath = currentDir + "/src/main/resources/people.tsv";
        String outputPath1 = currentDir + "/src/main/resources/peopleSimple";
        String outputPath2 = currentDir + "/src/main/resources/peopleRepart";
        String outputPath3 = currentDir + "/src/main/resources/peopleCoalesce";
        
        // Create a test file if it doesn't exist
        try {
            // Read text file
            JavaRDD<String> textFile = sparkContext.textFile(inputPath);
            
            // Map to Person objects
            JavaRDD<Person> people = textFile.map(line -> {
                String[] parts = line.split("~");
                Person person = new Person();
                person.setName(parts[0]);
                person.setAge(Integer.parseInt(parts[1].trim()));
                person.setOccupation(parts[2]);
                return person;
            });
            
            // Print each person
            System.out.println("=== People (map) ===");
            people.foreach(p -> System.out.println(p));
            
            // Map partitions to Person objects
            JavaRDD<Person> peoplePart = textFile.mapPartitions(p -> {
                ArrayList<Person> personList = new ArrayList<Person>();
                while (p.hasNext()) {
                    String[] parts = p.next().split("~");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    person.setOccupation(parts[2]);
                    personList.add(person);
                }
                return personList.iterator();
            });
            
            // Print each person from mapPartitions
            System.out.println("=== People (mapPartitions) ===");
            peoplePart.foreach(p -> System.out.println(p));
            
            // Save as text files with different partitioning
            people.saveAsTextFile(outputPath1);
            people.repartition(1).saveAsTextFile(outputPath2);
            people.coalesce(1).saveAsTextFile(outputPath3);
            
            System.out.println("Files saved to:");
            System.out.println(outputPath1);
            System.out.println(outputPath2);
            System.out.println(outputPath3);
        } catch (Exception e) {
            System.err.println("Error processing text file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close resources
            spark.stop();
        }
    }
}