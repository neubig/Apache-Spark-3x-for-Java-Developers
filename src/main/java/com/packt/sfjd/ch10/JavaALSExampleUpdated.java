package com.packt.sfjd.ch10;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

/**
 * Example of using ALS (Alternating Least Squares) for movie recommendation
 * Based on: examples/src/main/java/org/apache/spark/examples/ml/JavaALSExample.java
 */
public class JavaALSExampleUpdated {

    public static void main(String[] args) {
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("JavaALSExample")
            .getOrCreate();

        try {
            // Download the MovieLens dataset if it doesn't exist
            downloadMovieLensDataset();
            
            // Load ratings data from CSV file
            JavaRDD<Rating> ratingsRDD = spark
                .read()
                .textFile("data/ml-latest-small/ratings.csv")
                .javaRDD()
                .filter(str -> !str.contains("userId"))  // Skip header
                .map((Function<String, Rating>) Rating::parseRating);
            
            // Convert RDD to DataFrame
            Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
            
            System.out.println("Sample of ratings data:");
            ratings.show(5);
            
            // Split data into training and test sets
            Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2}, 12345);
            Dataset<Row> training = splits[0];
            Dataset<Row> test = splits[1];
            
            System.out.println("Training set count: " + training.count());
            System.out.println("Test set count: " + test.count());
            
            // Build the recommendation model using ALS on the training data
            ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.01)
                .setUserCol("userId")
                .setItemCol("movieId")
                .setRatingCol("rating")
                .setColdStartStrategy("drop");  // Handle missing values in test data
            
            System.out.println("Training ALS model...");
            ALSModel model = als.fit(training);

            // Make predictions on test data
            System.out.println("Making predictions...");
            Dataset<Row> predictions = model.transform(test);
            
            System.out.println("Sample predictions:");
            predictions.select("userId", "movieId", "rating", "prediction").show(5);

            // Evaluate the model
            RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
                
            Double rmse = evaluator.evaluate(predictions);
            System.out.println("Root-mean-square error = " + rmse);
            
            // Generate top 10 movie recommendations for a subset of users
            System.out.println("Generating movie recommendations...");
            Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);
            Dataset<Row> userRecs = model.recommendForUserSubset(users, 10);
            
            System.out.println("Top 10 movie recommendations for sample users:");
            userRecs.show(false);
            
        } catch (Exception e) {
            System.err.println("Error in ALS example: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Stop Spark session
            spark.stop();
        }
    }
    
    /**
     * Downloads the MovieLens dataset if it doesn't exist
     */
    private static void downloadMovieLensDataset() {
        try {
            // Create directory for MovieLens data
            java.io.File dataDir = new java.io.File("data/ml-latest-small");
            if (!dataDir.exists()) {
                System.out.println("Downloading MovieLens dataset...");
                dataDir.mkdirs();
                
                // Download the dataset
                java.net.URL url = new java.net.URL("https://files.grouplens.org/datasets/movielens/ml-latest-small.zip");
                java.nio.file.Path zipPath = java.nio.file.Paths.get("data/ml-latest-small.zip");
                
                try (java.io.InputStream in = url.openStream()) {
                    java.nio.file.Files.copy(in, zipPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
                
                // Unzip the dataset
                java.util.zip.ZipFile zipFile = new java.util.zip.ZipFile(zipPath.toFile());
                java.util.Enumeration<? extends java.util.zip.ZipEntry> entries = zipFile.entries();
                
                while (entries.hasMoreElements()) {
                    java.util.zip.ZipEntry entry = entries.nextElement();
                    java.io.File entryFile = new java.io.File("data/" + entry.getName());
                    
                    if (entry.isDirectory()) {
                        entryFile.mkdirs();
                    } else {
                        entryFile.getParentFile().mkdirs();
                        try (java.io.InputStream in = zipFile.getInputStream(entry);
                             java.io.FileOutputStream out = new java.io.FileOutputStream(entryFile)) {
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = in.read(buffer)) > 0) {
                                out.write(buffer, 0, len);
                            }
                        }
                    }
                }
                
                zipFile.close();
                
                // Delete the zip file
                java.nio.file.Files.delete(zipPath);
                
                System.out.println("MovieLens dataset downloaded and extracted successfully.");
            }
        } catch (Exception e) {
            System.err.println("Error downloading MovieLens dataset: " + e.getMessage());
            e.printStackTrace();
        }
    }
}