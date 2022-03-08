package com.geekcap.javaworld.sparkexample;

import java.util.Arrays;

import org.apache.commons.math.FunctionEvaluationException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Function;
import scala.Tuple2;

public class MySparkKeyOperations {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SparkConf sconf = new SparkConf().setMaster("local").setAppName("MySpark Key Operations");
		JavaSparkContext jsc = new JavaSparkContext(sconf);
		JavaRDD<String> inputFile = jsc.textFile("testdata.txt");
		System.out.println(inputFile.count());
		
		//ForEach
		inputFile.foreach(new VoidFunction<String>() {
			public void call(String arg) throws Exception {
				System.out.println(arg);
			}
		});
		
		//MapToPair
		PairFunction<String,String,Integer> getPairedDataFunc = new PairFunction<String,String,Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String,Integer> call(String arg0) throws Exception {
				return new Tuple2(arg0,1);
			}
		};
		
		JavaPairRDD<String,Integer> mappedData = inputFile.mapToPair(getPairedDataFunc);
		System.out.println(mappedData.count());
		//mappedData.saveAsTextFile("C:\\Users\\chaya_000\\Desktop\\jw-osjp-spark-src\\mapToPairOP");

		
		//ReduceByeKey
		Function2<Integer,Integer,Integer> function2 = new Function2<Integer,Integer,Integer>(){
			private static final long serialVersionUID = 1L;
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		};
		
		/*Function2<String,String,String> function2 = new Function2<String,String,String>(){
		private static final long serialVersionUID = 1L;
		public String call(String arg0, String arg1) throws Exception {
			return null;
		}
		};*/

		//JavaPairRDD<String,Integer> reducedRDD = mappedData.reduceByKey(function2);
		JavaPairRDD<String,Integer> reducedRDD = mappedData.reduceByKey(function2,2);
		System.out.println(reducedRDD.count());
		for(Tuple2<String,Integer> indRDD: reducedRDD.collect()) {
			System.out.println(indRDD._1 + "  " + indRDD._2);
		}
		//reducedRDD.saveAsTextFile("C:\\Users\\chaya_000\\Desktop\\jw-osjp-spark-src\\reducedByKeyOP");
	}
}