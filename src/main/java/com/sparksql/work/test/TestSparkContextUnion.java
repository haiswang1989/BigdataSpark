package com.sparksql.work.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSON;
import com.sparksql.work.bean.TEvent;
import com.sparksql.work.constant.ConstInfo;

/**
 * 
 * @author haiswang
 *
 */
public class TestSparkContextUnion {

	/**
	 * @param args
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		System.out.println(System.getProperties());
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("TestSparkContextUnion");
		sparkConf.setMaster("local[8]");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sparkContext);
		
		FileSystem fileSystem = null;
		
		DataFrame allDataFrame = null;
		DataFrame tmpDataFrame = null;
		
		try {
			fileSystem = FileSystem.get(new Configuration());
			FileStatus[] fileStatus = fileSystem.listStatus(new Path(ConstInfo.HDFS_DATA_PATH));
			for (FileStatus fs : fileStatus) {
				String fileAbPath = fs.getPath().toString();
				System.out.println(fileAbPath);
				JavaRDD<TEvent> rdd = sparkContext.textFile(fileAbPath).filter(new Function<String, Boolean>() {
					@Override
					public Boolean call(String v1) throws Exception {
						return null!=v1 && !"".equals(v1);
					}
				}).map(new Function<String, TEvent>() {
					@Override
					public TEvent call(String v1) throws Exception {
						TEvent tEvent = null;
						try {
							tEvent = JSON.parseObject(v1, TEvent.class);
						} catch(Exception e) {
						}
						
						return tEvent;
					}
				}).filter(new Function<TEvent, Boolean>() {
					@Override
					public Boolean call(TEvent v1) throws Exception {
						return null != v1;
					}
				});
				
				if(null==allDataFrame) {
					allDataFrame = sqlContext.createDataFrame(rdd, TEvent.class);
				} else {
					tmpDataFrame = sqlContext.createDataFrame(rdd, TEvent.class);
					
					/**
					 * DataFrame的unionAll和SparkContext的union在进行大量次数的操作会出现栈溢出的Error
					 */
					//allDataFrame.unionAll(tmpDataFrame);
					
					JavaRDD<Row> allRdd = sparkContext.union(allDataFrame.javaRDD() ,tmpDataFrame.javaRDD());
					allDataFrame = sqlContext.createDataFrame(allRdd, tmpDataFrame.schema());
				}
			}
			
			allDataFrame.show();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		try {
			fileSystem.close();
			sparkContext.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
