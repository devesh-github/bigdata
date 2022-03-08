package com.geekcap.javaworld.sparkexample;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FindFirstStudent {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SparkConf sconf = new SparkConf().setMaster("local").setAppName(
				"MySpark First Student");

		SparkContext sc = new SparkContext(sconf);
		JavaSparkContext jsc = new JavaSparkContext(sc);

		JavaRDD<String> inputDataRDD = jsc
				.textFile("C:\\Users\\chaya_000\\Desktop\\jw-osjp-spark-src\\samplestudentdata.txt");

		for (String data : inputDataRDD.collect()) {
			System.out.println(data);
		}

		List<String> strList = inputDataRDD.takeSample(true, 4);
		for (String str : strList) {
			System.out.println(str);
		}

		JavaPairRDD<String, Integer> studenNumberPair = inputDataRDD
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String arg)
							throws Exception {
						String[] arr = arg.split(" ");
						return new Tuple2(arr[0], Integer.valueOf(arr[1]));
					}
				});

		System.out.println(studenNumberPair.take(3).get(0));

	}
}
