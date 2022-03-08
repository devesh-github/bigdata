package com.dev.spark;

import org.apache.commons.math.stat.descriptive.rank.Max;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class FindMaxSalary {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		//Initialization
		SparkConf sconf = new SparkConf().setMaster("local").setAppName("Employee Data");
		SparkContext sc = new SparkContext(sconf);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		//DataLoad
		//JavaRDD<String> inpRDD = jsc.textFile("C://Users//chaya_000//Desktop//jw-osjp-spark-src//timepasstecchies.txt",3);
		JavaRDD<String> inpRDD = jsc.textFile("H://DevesH//workspace_Java//BigData-Spark-Concepts//timepasstecchies.txt",3);
		
		//Test
		//System.out.println(">>>>>>>>>>>>>>>>"+inpRDD.count());
		
		//Custom Functions
		Function<String,Boolean> deptFilterFunc = new Function<String,Boolean>(){
			public Boolean call(String v1) throws Exception {
				if(v1.contains("fire"))
					return true;
				return false;
			}
		};
		VoidFunction<String> voidFuncForforeach = new VoidFunction<String>(){
				public void call(String t) throws Exception {
					System.out.println(">>>>>>>>>>>>>>>>>>>>>"+t);
				}
		};
		Function<String,String> groupByInpFunc = new Function<String,String>(){
			public String call(String v1) throws Exception {
				return v1.split(",")[3];
			}
		};
		VoidFunction<Tuple2<String,Iterable<String>>> voidFuncForforeachGroupRDD = new VoidFunction<Tuple2<String,Iterable<String>>>(){
			public void call(Tuple2<String, Iterable<String>> t)
					throws Exception {
				System.out.println("Key>>>>>>>>>>>>>>"+t._1);
				for(String str:t._2) {
					System.out.println("Values>>>>>>>>>>>>>>"+str);
				}
			}
		};
		Function2<String,String,Double> reduceByFunc = new Function2<String,String,Double>(){
			public Double call(String v1, String v2) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		};
		Function<String, String> mapFunc = new Function<String, String>() {
			public String call(String t) throws Exception {
				return t;
			}
		};
		PairFunction<String,String,Double> pairFunc = new PairFunction<String,String,Double>() {
			public Tuple2<String, Double> call(String t) throws Exception {
				Double sal;
				if(t.split(",")[7]!=null && !t.split(",")[7].isEmpty())
					sal = Double.parseDouble(t.split(",")[7]);
				else 
					sal = null;
				return new Tuple2(t.split(",")[3],sal);
			}
		};
		Function2<Double,Double,Double> func2ForReduceByKey = new Function2<Double, Double, Double>() {
			public Double call(Double v1, Double v2) throws Exception {
				if(v1!=null && v1>v2)
					return v1;
				else
					return v2;
			}
		};
		VoidFunction<Tuple2<String,Double>> voidFuncForforeachReducedRDD = new VoidFunction<Tuple2<String,Double>>(){
			public void call(Tuple2<String, Double> t)
					throws Exception {
				System.out.println("Key>>>>>>>>>>>>>>"+t._1);
				System.out.println("Values>>>>>>>>>>>>>>"+t._2);
			}
		};
		
		
		//1st Level Filter
		JavaRDD<String> filteredRDD = inpRDD.filter(deptFilterFunc);
		//Test
		//filteredRDD.foreach(voidFuncForforeach);
		
		//Grouping By key
		JavaPairRDD<String,Iterable<String>> groupedRDD = inpRDD.groupBy(groupByInpFunc);
		//Test
		//groupedRDD.foreach(voidFuncForforeachGroupRDD);
		
		//ReduceByKey
		JavaRDD<String> mappedRDD = inpRDD.map(mapFunc);
		JavaPairRDD<String,Double> pairedRDD = mappedRDD.mapToPair(pairFunc);
		JavaPairRDD<String,Double> finalRDD = pairedRDD.reduceByKey(func2ForReduceByKey);
		finalRDD.foreach(voidFuncForforeachReducedRDD);
		
		System.out.println(finalRDD.toDebugString());
		
	}

}
