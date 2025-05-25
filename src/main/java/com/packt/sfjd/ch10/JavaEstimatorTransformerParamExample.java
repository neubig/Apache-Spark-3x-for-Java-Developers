package com.packt.sfjd.ch10;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Example demonstrating Estimator, Transformer, and Param concepts in Spark ML
 */
public class JavaEstimatorTransformerParamExampleUpdated {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("JavaEstimatorTransformerParamExample")
            .getOrCreate();
            
        // Set up logging
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        
        try {
            // Prepare training data
            List<Row> dataTraining = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)),
                RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5))
            );
            
            // Define schema for the data
            StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
            });
            
            // Create DataFrame from the data
            Dataset<Row> training = spark.createDataFrame(dataTraining, schema);
            
            System.out.println("Training data:");
            training.show(false);

            // Create a LogisticRegression instance (this is an Estimator)
            LogisticRegression lr = new LogisticRegression();
            
            // Print out the parameters, documentation, and any default values
            System.out.println("LogisticRegression parameters:\n" + lr.explainParams() + "\n");

            // Set parameters using setter methods
            lr.setMaxIter(10)
              .setRegParam(0.01)
              .setFamily("binomial");

            // Learn a LogisticRegression model using the parameters stored in lr
            System.out.println("Training Model 1...");
            LogisticRegressionModel model1 = lr.fit(training);
            
            // View the parameters used during fit()
            System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

            // Alternatively specify parameters using a ParamMap
            ParamMap paramMap = new ParamMap()
                .put(lr.maxIter().w(20))  // Specify 1 Param
                .put(lr.maxIter(), 30)    // This overwrites the original maxIter
                .put(lr.regParam().w(0.1), lr.threshold().w(0.55));  // Specify multiple Params

            // Combine ParamMaps
            ParamMap paramMap2 = new ParamMap()
                .put(lr.probabilityCol().w("myProbability"));  // Change output column name
            ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

            // Learn a new model using the paramMapCombined parameters
            System.out.println("Training Model 2...");
            LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
            System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

            // Prepare test data
            List<Row> dataTest = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
                RowFactory.create(0.0, Vectors.dense(3.0, 2.0, -0.1)),
                RowFactory.create(1.0, Vectors.dense(0.0, 2.2, -1.5))
            );
            Dataset<Row> test = spark.createDataFrame(dataTest, schema);
            
            System.out.println("Test data:");
            test.show(false);

            // Make predictions on test data using the Transformer.transform() method
            System.out.println("Making predictions...");
            Dataset<Row> results = model2.transform(test);
            
            // Display results
            Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
            System.out.println("Predictions:");
            rows.show(false);
            
            System.out.println("Detailed prediction results:");
            for (Row r: rows.collectAsList()) {
                System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
            }
            
        } catch (Exception e) {
            System.err.println("Error in example: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Stop Spark session
            spark.stop();
        }
    }
}