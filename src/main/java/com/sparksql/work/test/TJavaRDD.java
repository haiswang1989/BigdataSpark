package com.sparksql.work.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TJavaRDD {
	
	private static final String master = "local[8]";
	private static final String appName = "TJavaRDD";
	private static SparkConf sparkConfig = null;
	
	static {
		sparkConfig = new SparkConf();
		sparkConfig.setAppName(appName).setMaster(master);
	}
	
	/**
	 * @param args
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> lines = sparkContext.textFile("file:///D:/spark-data/*.txt");
		
		//这是Transformation操作,不回立即执行,只能等到Actions操作的时候才会执行
		//如果不执行rdd的Action操作如collect(),那么call方法就不回被回调
		//"come to call" 被多次打印,也验证了call是针对Block回调,而不是RDD
		JavaRDD<Object> rddMapPartitions = lines.mapPartitions(new FlatMapFunction<Iterator<String>, Object>() {
			
			/**
			 * 这边是每个Block的迭代操作
			 * @param sIter
			 * @return
			 * @throws Exception
			 */
			public Iterable<Object> call(Iterator<String> sIter) throws Exception {
				System.out.println("come to mapPartitions call.");
				List<Object> objList = new ArrayList<Object>();
				while(sIter.hasNext()) {
					String element = sIter.next();
					objList.add(element);
				}
				
				return objList;
			}
		});
		
		JavaRDD<Object> rddMap = lines.map(new Function<String, Object>() {
			@Override
			public Object call(String v1) throws Exception {
				System.out.println("come to map call.");
				return v1;
			}
		});
		
		JavaRDD<String> rddFlatMap = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String t) throws Exception {
				System.out.println("come to flatMap call.");
				return Arrays.asList(t.split(","));
			}
		});
		
		JavaRDD<String> rddGlom = lines.glom().map(new Function<List<String>, String>() {
			@Override
			public String call(List<String> elements) throws Exception {
				StringBuilder sb = new StringBuilder();
				for (String element : elements) {
					sb.append(element).append(",");
				}
				return sb.toString();
			}
		});
		
		//Actiom操作
		rddGlom.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
		JavaPairRDD<String, String> pairRDDMapToPair = lines.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String input) throws Exception {
				String[] arrays = input.split(",");
				return new Tuple2<String, String>(arrays[0], arrays[1]);
			}
		});
		
		JavaPairRDD<String, String> pairRDDFlatMapToPair = lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
			@Override
			public Iterable<Tuple2<String, String>> call(String input)
					throws Exception {
				List<Tuple2<String, String>> tuples = new ArrayList<>();
				String[] tmpArrays = input.split(",");
				for (String str : tmpArrays) {
					String[] array = str.split(":");
					tuples.add(new Tuple2<String ,String>(array[0], array[1]));
				}
				
				return tuples;
			}
		});
		
		JavaPairRDD<String, String> pairRDDMapPartitionsToPair = lines.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
			@Override
			public Iterable<Tuple2<String, String>> call(Iterator<String> t)
					throws Exception {
				return null;
			}
		});
		
		JavaPairRDD<String, Integer> pairs = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
			@Override
			public Iterable<Tuple2<String, Integer>> call(String input)
					throws Exception {
				
				List<Tuple2<String, Integer>> tuples = new ArrayList<>();
				for (String word : input.split(",")) {
					tuples.add(new Tuple2<String, Integer>(word, 1));
				}
				
				return tuples;
			}
		});
		
		JavaPairRDD<String, Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1 + v2;
			}
		});
		
		result.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + ":" + t._2);
			}
		});
		
		
		//非Action操作
		//rdd.cache();
		
		//Action操作
		//rddMapPartitions.collect();
		
		//rddMap.collect();
		
		//rddFlatMap.collect();
		
		
		/***************************************************/
		
		//sparkContext.p
		
		
		sparkContext.close();
	}

}
