package com.packt.sfjd.ch10;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.FilterFunction;

import java.io.Serializable;

public class FlightDelay implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\hadoop");
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);

		SparkSession spark = SparkSession.builder().master("local").appName("Flight Delay")
				.config("spark.sql.warehouse.dir", "file:///E:/hadoop/warehouse").getOrCreate();

		Dataset<Row> flight2007 = spark.read().option("header", "true").option("inferSchema", "true")
				.csv("src/main/resources/2007.csv");

		Dataset<Row> flight2008 = spark.read().option("header", "true").option("inferSchema", "true")
				.csv("src/main/resources/2008.csv");

		String header = flight2007.first().mkString(",");

		// Double.parseDouble(p[14]),
		// (p[15].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[15]), p[16],
		// (p[18].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[18])
		JavaRDD<Flight> trainingRDD = flight2007
				.filter((FilterFunction<Row>) x -> !x.mkString(",").equalsIgnoreCase(header))
				.javaRDD()
				.map(x -> x.mkString(",").split(","))
				.filter(x -> x[21].equalsIgnoreCase("0"))
				.filter(x -> x[17].equalsIgnoreCase("ORD"))
				.filter(x -> x[14].equalsIgnoreCase("NA"))
				.map(p -> new Flight(
						p[1],                                                // month
						p[2],                                                // dayofMonth
						p[3],                                                // dayOfWeek
						getMinuteOfDay(p[4]),                                // depTime
						getMinuteOfDay(p[6]),                                // crsDepTime
						0,                                                   // arrTime (not used)
						0,                                                   // crsArrTime (not used)
						"",                                                  // uniqueCarrier (not used)
						0,                                                   // actualElapsedTime (not used)
						(p[15].equalsIgnoreCase("NA")) ? 0 : Integer.parseInt(p[15]), // crsElapsedTime
						0,                                                   // airTime (not used)
						0.0,                                                 // arrDelay (not used)
						(p[19].equalsIgnoreCase("NA")) ? 0 : Integer.parseInt(p[19]), // depDelay
						"ORD",                                               // origin
						(p[18].equalsIgnoreCase("NA")) ? 0 : Integer.parseInt(p[18])  // distance
				));

		Dataset<Row> trainingData = spark.createDataFrame(trainingRDD, Flight.class);

		JavaRDD<Flight> testRDD = flight2008
				.filter((FilterFunction<Row>) x -> !x.mkString(",").equalsIgnoreCase(header))
				.javaRDD()
				.map(x -> x.mkString(",").split(","))
				.filter(x -> x[21].equalsIgnoreCase("0"))
				.filter(x -> x[17].equalsIgnoreCase("ORD"))
				.filter(x -> x[14].equalsIgnoreCase("NA"))
				.map(p -> new Flight(
						p[1],                                                // month
						p[2],                                                // dayofMonth
						p[3],                                                // dayOfWeek
						getMinuteOfDay(p[4]),                                // depTime
						getMinuteOfDay(p[6]),                                // crsDepTime
						0,                                                   // arrTime (not used)
						0,                                                   // crsArrTime (not used)
						"",                                                  // uniqueCarrier (not used)
						0,                                                   // actualElapsedTime (not used)
						(p[15].equalsIgnoreCase("NA")) ? 0 : Integer.parseInt(p[15]), // crsElapsedTime
						0,                                                   // airTime (not used)
						0.0,                                                 // arrDelay (not used)
						(p[19].equalsIgnoreCase("NA")) ? 0 : Integer.parseInt(p[19]), // depDelay
						"ORD",                                               // origin
						(p[18].equalsIgnoreCase("NA")) ? 0 : Integer.parseInt(p[18])  // distance
				));

		Dataset<Row> testData = spark.createDataFrame(testRDD, Flight.class);

		StringIndexerModel labelIndexer = new StringIndexer().setInputCol("cancelled").setOutputCol("indexedLabel")
				.fit(trainingData);

		StringIndexerModel monthIndexer = new StringIndexer().setInputCol("month").setOutputCol("monthIndex")
				.fit(trainingData);

		StringIndexerModel dayofmonthIndexer = new StringIndexer().setInputCol("dayofmonth")
				.setOutputCol("dayofmonthIndex").fit(trainingData);

		StringIndexerModel dayofweekIndexer = new StringIndexer().setInputCol("dayofweek").setOutputCol("dayofweekIndex")
				.fit(trainingData);

		VectorAssembler assembler = new VectorAssembler().setInputCols(new String[] { "monthIndex", "dayofmonthIndex",
				"dayofweekIndex", "crsdeptime", "crsarrtime", "crselapsedtime", "distance", "depdelay" })
				.setOutputCol("features");

		DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("features");

		IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel")
				.setLabels(labelIndexer.labels());

		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] { labelIndexer, monthIndexer, dayofmonthIndexer, dayofweekIndexer,
						assembler, dt, labelConverter });

		PipelineModel model = pipeline.fit(trainingData);

		Dataset<Row> predictions = model.transform(testData);

		predictions.select("predictedLabel", "cancelled", "features").show(5);

		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel")
				.setPredictionCol("prediction").setMetricName("accuracy");
		double accuracy = evaluator.evaluate(predictions);
		System.out.println("Test Error = " + (1.0 - accuracy));

	}

	private static int getMinuteOfDay(String timeStr) {
		if (timeStr.length() < 3) {
			int hour = 0;
			int minute = Integer.parseInt(timeStr);
			return hour * 60 + minute;
		} else if (timeStr.length() == 3) {
			int hour = Integer.parseInt(timeStr.substring(0, 1));
			int minute = Integer.parseInt(timeStr.substring(1, 3));
			return hour * 60 + minute;
		} else {
			int hour = Integer.parseInt(timeStr.substring(0, 2));
			int minute = Integer.parseInt(timeStr.substring(2, 4));
			return hour * 60 + minute;
		}
	}
}