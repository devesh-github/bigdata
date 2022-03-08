package com.geekcap.javaworld.sparkexample;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Function;
import scala.Tuple2;

public class MySparkWordCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SparkConf sconf = new SparkConf().setMaster("local").setAppName(
				"MySpark WordCount");

		JavaSparkContext jsc = new JavaSparkContext(sconf);

		JavaRDD<String> inputFile = jsc.textFile("sampledata.txt");
		System.out.println(inputFile.count());
		
		//Filter
		JavaRDD<String> filteredRDD = inputFile.filter(new org.apache.spark.api.java.function.Function<String, Boolean>() {
			public Boolean call(String arg0) throws Exception {
				if(arg0.contains("X"))
					return false;
				return true;
			}
		});
		
		for (String inputFileFlatMapStr : filteredRDD.collect()) {
			//System.out.println(filteredRDD.take(3).get(0));
			System.out.println("FilteredRDD::"+inputFileFlatMapStr);
		}
		
		//FlatMap
		JavaRDD<String> inputFileFlatMap = inputFile
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterable<String> call(String arg0) throws Exception {
						System.out.println(arg0);
						return Arrays.asList(arg0.split(" "));
					}
				});
		System.out.println(inputFileFlatMap.count());

		for (String inputFileFlatMapStr : inputFileFlatMap.collect()) {
			System.out.println(inputFileFlatMap.take(3).get(0));
			System.out.println(inputFileFlatMapStr);
		}

		JavaPairRDD<String, Integer> inputFileFlatMapMapToPair = inputFileFlatMap
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String arg0)
							throws Exception {
						System.out.println(arg0);
						return new Tuple2(arg0, 1);
					}
				});
		// System.out.println(inputFileFlatMapMapToPair.count());
		// System.out.println(inputFileFlatMapMapToPair.take(3).get(0));

		JavaPairRDD<String, Integer> reducedCounts = inputFileFlatMapMapToPair
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer x, Integer y) {
						//System.out.println(x + " " + y);
						return x + y;
					}
				});
		//System.out.println(reducedCounts.count());
		//System.out.println(reducedCounts.take(5).get(0));
		
		// inputFileFlatMap.saveAsTextFile("C:\\Users\\chaya_000\\Desktop\\jw-osjp-spark-src\\output");
		
	}

}