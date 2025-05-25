package com.packt.sfjd.ch4;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount_1_7 {
        public static void main(String[] args) throws Exception {
                System.out.println(System.getProperty("hadoop.home.dir"));
                String inputPath = args[0];
                String outputPath = args[1];
                FileUtils.deleteQuietly(new File(outputPath));

                SparkConf conf = new SparkConf().setAppName("sparkwordcount1.7").setMaster("local[*]");
                JavaSparkContext sc = new JavaSparkContext(conf);

                JavaRDD<String> rdd = sc.textFile(inputPath);

                JavaPairRDD<String, Integer> counts = rdd
                        .flatMap(x -> Arrays.asList(x.split(" ")).iterator())
                        .mapToPair(x -> new Tuple2<>(x, 1))
                        .reduceByKey((x, y) -> x + y);
                        
                counts.saveAsTextFile(outputPath);
                sc.close();
        }
}