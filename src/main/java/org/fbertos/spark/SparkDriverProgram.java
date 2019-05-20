package org.fbertos.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


public class SparkDriverProgram {
    @SuppressWarnings("unchecked")
	public static void main(String args[]) throws FileNotFoundException, UnsupportedEncodingException {

    	SparkSession ss = SparkSession.builder().appName("spark-example-dataset").getOrCreate();
    	
    	Dataset<Row> df = ss.read().option("header", "true").csv("/home/fbertos/workspace/spark-example-dataset/data.csv");
    	
    	//df.printSchema();
    	
    	df.createOrReplaceTempView("OCCUPANCY_RAW");

    	Dataset<Row> df2 = ss.read().option("header", "true").csv("/home/fbertos/workspace/spark-example-dataset/data.csv");
    	df2.createOrReplaceTempView("OCCUPANCY_RAW2");
    	
    	Dataset<Row> data = ss.sql("select a.id, b.id FROM OCCUPANCY_RAW a, OCCUPANCY_RAW2 b where a.id = b.id");
    	
    	data.show();
    	
    	ss.close();
    	
    	
   }
}