package com.sparksql.work.test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

import com.google.common.base.Optional;

public class TKVTransformation {
	
	private static final String master = "local[8]";
	private static final String appName = "TCache";
	private static SparkConf sparkConfig = null;
	
	static {
		sparkConfig = new SparkConf();
		sparkConfig.setAppName(appName).setMaster(master);
	}
	
	/**
	 * @param args
	 */
	@SuppressWarnings({ "serial" })
	public static void main(String[] args) {
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
		
		List<Tuple2<String, String>> pairs = new ArrayList<Tuple2<String,String>>();
		pairs.add(new Tuple2<String, String>("name", "wanghaisheng"));
		pairs.add(new Tuple2<String, String>("age", "28"));
		pairs.add(new Tuple2<String, String>("sex", "male"));
		
		pairs.add(new Tuple2<String, String>("name", "wanghaiming"));
		pairs.add(new Tuple2<String, String>("age", "32"));
		pairs.add(new Tuple2<String, String>("sex", "male"));
		
		JavaPairRDD<String, String> pairRDD = sparkContext.parallelizePairs(pairs);
		/**
		 * mapValues是对所有的value进行处理
		 */
		JavaPairRDD<String, String> pairRDD_ = pairRDD.mapValues(new Function<String, String>() {
			@Override
			public String call(String value) throws Exception {
				// TODO Auto-generated method stub
				return value + "_";
			}
		});
		
		pairRDD_.foreach(new VoidFunction<Tuple2<String,String>>() {
			
			@Override
			public void call(Tuple2<String, String> t) throws Exception {
				System.out.println(t._1 + ":" + t._2);
			}
		});
		
		/***********************************************************/
		
		//combineByKey
		JavaPairRDD<String, List<String>> javaPairRDD = pairRDD.combineByKey(new Function<String, List<String>>() { //针对K-V对
			@Override
			public List<String> call(String v1) throws Exception { //第一次遇到值为K的key
				List<String> ret = new ArrayList<>();
				ret.add(v1);
				return ret;
			}
		}, new Function2<List<String>, String, List<String>>() { //不是第一次遇到值为K的key
			@Override
			public List<String> call(List<String> v1, String v2)
					throws Exception {
				v1.add(v2);
				return v1;
			}
		}, new Function2<List<String>, List<String>, List<String>>() { //由于是分布式计,RDD的每个分区单独进行combineByKey ,所以嘴和需要对每个Partition的结果进行嘴和的聚合
			@Override
			public List<String> call(List<String> v1, List<String> v2)
					throws Exception {
				v1.addAll(v2);
				return v1;
			}
		});
		
		javaPairRDD.foreach(new VoidFunction<Tuple2<String,List<String>>>() {
			@Override
			public void call(Tuple2<String, List<String>> tuple) throws Exception {
				String key = tuple._1;
				List<String> val = tuple._2;
				System.out.println(key + " : " + val);
			}
		});
		
		/***************************************************/
		
		//reduceByKey
		List<String> words = new LinkedList<>();
		words.add("wang");
		words.add("hai");
		words.add("sheng");
		words.add("wang");
		words.add("hai");
		words.add("ming");
		JavaRDD<String> wordsRDD = sparkContext.parallelize(words);
		
		//将word--->word/count
		JavaPairRDD<String, Integer> wordsCountRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		//合并count
		JavaPairRDD<String, Integer> wordsCount = wordsCountRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		wordsCount.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + " : " + t._2);
			}
		});
		
		/***************************************************************************/
		//partitionBy
		JavaPairRDD<String, Integer> wordsPatitionRDD = sparkContext.parallelize(words).mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
		
		
		List<Partition> wordsPartitions = wordsPatitionRDD.partitions();
		System.out.println("partition count : " + wordsPartitions.size());
		for (Partition partition : wordsPartitions) {
			//partition的编号
			System.out.println("index : " + partition.index());
		}
		
		/******************************************************************************/
		//cogroup聚集,笛卡尔积
		List<String> firstWords = new LinkedList<>();
		List<String> secondsList = new LinkedList<>();
		firstWords.add("wang");
		firstWords.add("hai");
		firstWords.add("sheng");
		
		secondsList.add("wang");
		secondsList.add("hai");
		secondsList.add("ming");
		
		JavaRDD<String> firstWordsRDD = sparkContext.parallelize(firstWords);
		JavaRDD<String> secondWorJavaRDD = sparkContext.parallelize(secondsList);
		
		PairFunction<String, String, Integer> pairFunction = new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		};
		
		JavaPairRDD<String, Integer> firstWordsPairRDD = firstWordsRDD.mapToPair(pairFunction);
		JavaPairRDD<String, Integer> secondWordsPairRDD = secondWorJavaRDD.mapToPair(pairFunction);
		JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> newWordsPairRDD = firstWordsPairRDD.cogroup(secondWordsPairRDD);
		
		newWordsPairRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<Iterable<Integer>,Iterable<Integer>>>>() {
			@Override
			public void call(Tuple2<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> tuple)
					throws Exception {
				
				String key = tuple._1;
				Tuple2<Iterable<Integer>, Iterable<Integer>> valTuple = tuple._2;
				Iterable<Integer> valTupleKey = valTuple._1;
				Iterable<Integer> valTupleVal = valTuple._2;
				System.out.println(key + " : " + valTupleKey + " : " + valTupleVal);
			}
		});
		/**************************************************************/
		//join
		JavaPairRDD<String, Tuple2<Integer, Integer>> newJoinedWordsPairRDD = firstWordsPairRDD.join(secondWordsPairRDD);
		
		JavaPairRDD<String, Integer> count =  newJoinedWordsPairRDD.mapValues(new Function<Tuple2<Integer,Integer>, Integer>() {
			@Override
			public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
				System.out.println(v1._1 == null);
				System.out.println(v1._2 == null);
				return v1._1 + v1._2;
			}
		});
		
		count.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + ":" + t._2);
			}
		});
		
		newJoinedWordsPairRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,Integer>>>() {
			@Override
			public void call(Tuple2<String, Tuple2<Integer, Integer>> tuple)
					throws Exception {
				String key = tuple._1;
				Tuple2<Integer, Integer> valTuple = tuple._2;
				
				Integer valTupleKey = valTuple._1;
				Integer valTupleVal = valTuple._2;
				System.out.println(key + ":" + valTupleKey + ":" + valTupleVal);
			}
		});
		
		/*
		JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftOuterJoinResult = firstWordsPairRDD.leftOuterJoin(secondWordsPairRDD);
		leftOuterJoinResult.foreach(new VoidFunction<Tuple2<String,Tuple2<Integer,Optional<Integer>>>>() {
			@Override
			public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> tuple)
					throws Exception {
				String key = tuple._1;
				Tuple2<Integer, Optional<Integer>> valTuple = tuple._2;
				Integer valTupleKey = valTuple._1;
				Integer valTupleVal = valTuple._2.get();
				
				System.out.println(key + ":" + valTupleKey + ":" + valTupleVal);
			}
		});
		*/
		
		sparkContext.close();
		
		
	}

}
