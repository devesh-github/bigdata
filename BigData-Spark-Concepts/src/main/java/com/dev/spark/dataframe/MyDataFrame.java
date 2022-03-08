package com.dev.spark.dataframe;

import java.sql.SQLClientInfoException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.avro.*;

public class MyDataFrame {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		SparkConf sonf = new SparkConf().setMaster("local").setAppName("DataFrameExp");
		SparkContext sc = new SparkContext(sonf);
		SQLContext sctx = new SQLContext(new JavaSparkContext(sc));
		
		DataFrame df = sctx.read().json("./StudentActivity.json");
		df.show();
		System.out.println(df.count());
		System.out.println(df.schema());
		df.filter("student_id=100").show();
		df.select("student_id").show();
		df.explain();
		
		RDD<Row> rowRDD = df.rdd();
		System.out.println(rowRDD.first().toString());
		System.out.println(rowRDD.take(0).toString());
		
		/*DataFrame avrodf = sctx.read().format("com.databricks.spark.avro").load("./episodes.avro");
		avrodf.show();
		avrodf.schema();*/
		/*avrodf.filter("student_id=100").show();
		avrodf.select("student_id").show();*/
		
	}

}
