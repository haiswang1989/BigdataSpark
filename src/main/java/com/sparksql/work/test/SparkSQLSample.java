package com.sparksql.work.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SparkSQLSample {

	/**
	 * @param args
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("SparkSQL")
				 //local:表示在本地起一个线程跑
				 //local[4]:表示本地跑,该Executor有4个cores
		         //"spark://master:7077":表示到spark集群上跑
				 .setMaster("local"); 
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(ctx);
		
		//历史数据
		JavaRDD<Person> personHistory = ctx.textFile("hdfs://master:9000/data/spark/user/user_1466491267478.txt").map(new Function<String, Person>() {
			
			@Override
			public Person call(String line) throws Exception {
				
				String[] infos = line.split(",");
				Person person = new Person();
				person.setId(infos[0]);
				person.setName(infos[1]);
				person.setSex(infos[2]);
				person.setAge(Integer.parseInt(infos[3]));
				return person;
			}
		});
		
		//新数据
		JavaRDD<Person> personNew = ctx.textFile("hdfs://master:9000/data/spark/user/user_1466491367478.txt").map(new Function<String, Person>() {
			
			@Override
			public Person call(String line) throws Exception {
				
				String[] infos = line.split(",");
				Person person = new Person();
				person.setId(infos[0]);
				person.setName(infos[1]);
				person.setSex(infos[2]);
				person.setAge(Integer.parseInt(infos[3]));
				return person;
			}
		});
		
		
		DataFrame userHistoryDataFrame = sqlContext.createDataFrame(personHistory, Person.class);
		userHistoryDataFrame.registerTempTable("person_history");
		DataFrame resultHistory = sqlContext.sql("select * from person_history");
		resultHistory.show();
		
		DataFrame userNewDataFrame = sqlContext.createDataFrame(personNew, Person.class);
		userNewDataFrame.registerTempTable("person_new");
		DataFrame resultNew = sqlContext.sql("select * from person_new");
		resultNew.show();
		
		
		
		//进行数据的合并
//		DataFrame combineDataFrame = userHistoryDataFrame.unionAll(userNewDataFrame);
//		combineDataFrame.registerTempTable("person_combine");
//		DataFrame resultCombine = sqlContext.sql("select * from person_combine");
//		resultCombine.show();
		
		userHistoryDataFrame.schema();
		
		JavaRDD<Row> rdd = ctx.union(userHistoryDataFrame.javaRDD() ,userNewDataFrame.javaRDD());
		DataFrame combineDataFrame = sqlContext.createDataFrame(rdd, userHistoryDataFrame.schema());
		combineDataFrame.show();
		
		/*
		//前两条记录
		resultCombine = resultCombine.limit(2);
		resultCombine.show();
		
		//count()函数
		DataFrame result = sqlContext.sql("select count(1) cnt from person_combine");
		result.show();
		
		//条件查询
		result = sqlContext.sql("select * from person_combine where age < 30");
		result.show();
		
		//函数
		result = sqlContext.sql("select sum(age) from person_combine");
		result.show();
		
		//新数据
		JavaRDD<Occupation> occupations = ctx.textFile("hdfs://master:9000/data/spark/user/occupation.txt").map(new Function<String, Occupation>() {
			
			@Override
			public Occupation call(String line) throws Exception {
				
				String[] infos = line.split(",");
				Occupation occupation = new Occupation();
				occupation.setId(infos[0]);
				occupation.setOccupationName(infos[1]);
				return occupation;
			}
		});
		
		DataFrame occupationDataFrame = sqlContext.createDataFrame(occupations, Occupation.class);
		occupationDataFrame.registerTempTable("occupation");
		occupationDataFrame.show();
		
		result = sqlContext.sql("select a.*,b.occupationName from person_combine a left join occupation b where a.id=b.id");
		result.show();
		*/
	}

}
