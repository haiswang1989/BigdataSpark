package com.sparksql.work.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.alibaba.fastjson.JSON;

public class LoadFromHdfs {
	
	private static Configuration config = new Configuration();
	private static FileSystem fileSystem = null;
	static JavaRDD<String> allData = null;
	private static int count = 0;
	
	static {
		try {
			fileSystem = FileSystem.get(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	static class Person implements Serializable {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 9047991441391597298L;
		String name;
		String age;
		
		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getAge() {
			return age;
		}

		public void setAge(String age) {
			this.age = age;
		}

		public Person(String name ,String age) {
			this.name = name;
			this.age = age;
		}
		
		@Override
		public String toString() {
			return JSON.toJSONString(this);
		}
	}
	
	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName("LoadFromHdfs");
		sparkConfig.setMaster("local[8]");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
		List<String> words = new LinkedList<>();
		words.add("wanghaisheng,28");
		words.add("wanghaiming,31");
		words.add("wanghaifei,33");
		JavaRDD<String> rdd = sparkContext.parallelize(words);
		
		JavaRDD<Person> personRdd = rdd.map(new Function<String, Person>() {
			@Override
			public Person call(String v1) throws Exception {
				String[] tmpAry = v1.split(",");
				String name = tmpAry[0];
				String age = tmpAry[1];
				return new Person(name,age);
			}
		});
		
		personRdd.repartition(1).saveAsTextFile("file:///D:/spark-data/output/");
		
		sparkContext.close();
		
		
//		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(10000l));
//		
//		streamingContext
		
		
		/*
		FileStatus[] filesStatus = null;
		
		try {
			filesStatus = fileSystem.listStatus(new Path("hdfs://topgun-spark1-8367.lvs01.dev.ebayc3.com:9000/data/spark/trackevent/"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("size : " + filesStatus.length);
		
		Arrays.asList(filesStatus).forEach((FileStatus fileStatus) -> {
			String filePath = fileStatus.getPath().toString();
			if(null == allData) {
				allData = sparkContext.textFile(filePath);
			} else {
				JavaRDD<String> rdd = sparkContext.textFile(filePath);
				allData = sparkContext.union(allData ,rdd);
			}
		});
		
		System.out.println("--------------------------------------");
		
		
		long count = allData.count();
		
		
		System.out.println("count : " + count);
		
		sparkContext.close();
		
		try {
			fileSystem.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
	}

}
