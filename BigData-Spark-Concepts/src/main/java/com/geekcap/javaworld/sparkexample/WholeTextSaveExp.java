package com.geekcap.javaworld.sparkexample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

class FileGroupingTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	@Override
	protected Text generateActualKey(Text key, Text value) {
		return key;
	}

	@Override
	protected Text generateActualValue(Text key, Text value) {
		return value;
	}

	// returns a dynamic file name based on each RDD element
	@Override
	protected String generateFileNameForKeyValue(Text key, Text value,
			String name) {
		return key.toString() + "-" + name;
	}
}

public class WholeTextSaveExp {

	/**
	 * @param args
	 */

	public static class ConvertToWritableTypes implements
			PairFunction<Tuple2<String, String>, Text, Text> {
		public Tuple2<Text, Text> call(Tuple2<String, String> record) {
			return new Tuple2(new Text(record._1), new Text(record._2));
		}
	}

	public static void main(String[] args) {

		SparkConf sconf = new SparkConf().setMaster("local").setAppName(
				"MySpark WordCount");
		JavaSparkContext jsc = new JavaSparkContext(sconf);

		Configuration conf = new Configuration();

		JavaPairRDD<String, String> inputFile = jsc
				.wholeTextFiles("testsaveinput/");
		System.out.println(inputFile.count());

		// inputFile.saveAsTextFile("testwholetextsave/");
		
		JavaPairRDD<Text, Text> result = inputFile.mapToPair(new ConvertToWritableTypes());
		System.out.println(result.count());

		result.saveAsHadoopFile("testwholetextsave2/", String.class,
				String.class, FileGroupingTextOutputFormat.class, new JobConf(
						conf));

	}

}
