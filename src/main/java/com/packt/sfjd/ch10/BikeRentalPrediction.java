package com.packt.sfjd.ch10;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

/**
 * Bike Rental Prediction using Spark ML Pipeline
 * Based on: https://docs.cloud.databricks.com/docs/latest/sample_applications/index.html#Sample%20ML/MLPipeline%20Bike%20Dataset.html
 */
public class BikeRentalPredictionUpdated {

    public static void main(String[] args) {
        // Set up Spark session
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("BikeRentalPrediction")
                .getOrCreate();
                
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        // Load the dataset
        // We use the read method to read the data and set a few options:
        // - 'header': set to true to indicate that the first line of the CSV data file is a header
        Dataset<Row> ds = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .load("data/hour.csv");
        
        // Cache the dataset for better performance
        ds.cache();
        
        // Display sample data
        System.out.println("Sample data from the dataset:");
        ds.show(5);
        
        System.out.println("Our dataset has rows: " + ds.count());
        
        // Drop unnecessary columns
        Dataset<Row> df = ds.drop("instant").drop("dteday").drop("casual").drop("registered");
        
        System.out.println("Schema after dropping columns:");
        df.printSchema();
        
        // Convert string columns to appropriate types
        Dataset<Row> dformatted = df.select(
                col("season").cast(DataTypes.IntegerType),
                col("yr").cast(DataTypes.IntegerType),
                col("mnth").cast(DataTypes.IntegerType),
                col("hr").cast(DataTypes.IntegerType),
                col("holiday").cast(DataTypes.IntegerType),
                col("weekday").cast(DataTypes.IntegerType),
                col("workingday").cast(DataTypes.IntegerType),
                col("weathersit").cast(DataTypes.IntegerType),
                col("temp").cast(DataTypes.DoubleType),
                col("atemp").cast(DataTypes.DoubleType),
                col("hum").cast(DataTypes.DoubleType),
                col("windspeed").cast(DataTypes.DoubleType),
                col("cnt").cast(DataTypes.IntegerType)
        );
        
        System.out.println("Schema after type conversion:");
        dformatted.printSchema();
        
        // Split the data into training and test sets
        Dataset<Row>[] data = dformatted.randomSplit(new double[]{0.7, 0.3}, 12345);
        System.out.println("We have training examples count: " + data[0].count() + 
                           " and test examples count: " + data[1].count());
        
        // Get feature columns (all columns except 'cnt' which is the target)
        String[] featuresCols = dformatted.drop("cnt").columns();
        
        System.out.println("Feature columns:");
        for (String str : featuresCols) {
            System.out.println(str);
        }
        
        // Create ML Pipeline
        
        // Step 1: Assemble features into a single vector
        // This concatenates all feature columns into a single feature vector in a new column "rawFeatures"
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(featuresCols)
                .setOutputCol("rawFeatures");
                
        // Step 2: Index categorical features
        // This identifies categorical features and indexes them
        VectorIndexer vectorIndexer = new VectorIndexer()
                .setInputCol("rawFeatures")
                .setOutputCol("features")
                .setMaxCategories(4);
    
        // Step 3: Create the regression model
        // Takes the "features" column and learns to predict "cnt"
        GBTRegressor gbt = new GBTRegressor()
                .setLabelCol("cnt")
                .setSeed(12345);
                
        // Define a grid of hyperparameters to test:
        // - maxDepth: max depth of each decision tree in the GBT ensemble
        // - maxIter: iterations, i.e., number of trees in each GBT ensemble
        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(gbt.maxDepth(), new int[]{2, 5})
                .addGrid(gbt.maxIter(), new int[]{10, 100})
                .build();
                
        // Define an evaluation metric
        // This tells CrossValidator how well we are doing by comparing the true labels with predictions
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol(gbt.getLabelCol())
                .setPredictionCol(gbt.getPredictionCol());
        
        // Declare the CrossValidator, which runs model tuning for us
        CrossValidator cv = new CrossValidator()
                .setEstimator(gbt)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(3)  // Use 3-fold cross-validation
                .setSeed(12345);
                
        // Build the pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{vectorAssembler, vectorIndexer, cv});
                
        // Train the model
        System.out.println("Training the model...");
        PipelineModel pipelineModel = pipeline.fit(data[0]);
        
        // Make predictions on test data
        System.out.println("Making predictions on test data...");
        Dataset<Row> predictions = pipelineModel.transform(data[1]);
        
        // Show predictions
        System.out.println("Predictions:");
        predictions.select("cnt", "prediction").show(10);
        
        // Evaluate the model
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
        
        // Clean up
        sparkSession.stop();
    }
}