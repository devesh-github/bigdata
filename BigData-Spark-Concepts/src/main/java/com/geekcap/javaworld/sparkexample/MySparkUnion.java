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

public class MySparkUnion {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SparkConf sconf = new SparkConf().setMaster("local").setAppName(
				"MySpark WordCount");

		JavaSparkContext jsc = new JavaSparkContext(sconf);

		JavaRDD<String> inputFile = jsc.textFile("sampledata.txt");
		System.out.println(inputFile.count());
		
		//Union
		JavaRDD<Integer> distData1 = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
		JavaRDD<Integer> distData2 = jsc.parallelize(Arrays.asList(6, 7));
		JavaRDD<Integer> rddUnion = distData1.union(distData2);
		for(Integer in: rddUnion.collect()) {
			System.out.println(in);
		}

	}
}