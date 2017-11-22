package com.sparksql.work;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSON;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.sparksql.work.bean.TEvent;
import com.sparksql.work.constant.ConstInfo;
import com.sparksql.work.util.Utils;

/**
 * Full Statistics task
 * @author haiswang
 *
 */
public class FullStatisticsTask extends TimerTask implements Serializable {
	
	private static final long serialVersionUID = -6022107508461440952L;
	
	private transient JavaSparkContext javaSparkContext = null;
	private transient SQLContext sqlContext = null;
	private CouchbaseClient couchbaseClint = null;
	
	
	public FullStatisticsTask() {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Full_Statistics").setMaster("local[8]");
		javaSparkContext = new JavaSparkContext(sparkConf);
		sqlContext = new SQLContext(javaSparkContext);
		couchbaseClint = new CouchbaseClient();
	}
	
	
	@SuppressWarnings("serial")
	@Override
	public void run() {
		
		
		long t1 = System.currentTimeMillis();
		System.out.println("start full statistics");
		
		JavaRDD<String> rdd = javaSparkContext.textFile(ConstInfo.HDFS_DATA_PATH + "*.nb");
		
		// filter empty string
		rdd = rdd.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				return null!=v1 && !"".equals(v1);
			}
		});
		
		//convert string to TEvent object , may return null
		JavaRDD<TEvent> fullUserData = rdd.map(new Function<String, TEvent>() {
			@Override
			public TEvent call(String line) throws Exception {
				TEvent tEvent = null;
				try {
					tEvent = JSON.parseObject(line, TEvent.class);
				} catch (Exception e) {
					
				}
				
				return tEvent;
			}
		});
		
		// filter empty TEvent object
		fullUserData = fullUserData.filter(new Function<TEvent, Boolean>() {
			@Override
			public Boolean call(TEvent v1) throws Exception {
				return null!=v1;
			}
		});
		
		//create Dataframe
		DataFrame fullUserDataframe = sqlContext.createDataFrame(fullUserData, TEvent.class);
		fullUserDataframe.registerTempTable(ConstInfo.MT_USER_TABLE_FULL);
		
		//submit query job
		DataFrame result = sqlContext.sql(ConstInfo.HQL_FULL_STATISTICS);
		
		List<JsonObject> events = new ArrayList<>();
		
		//loop
		for (Row row : result.collect()) {
			String eventId = row.getString(0);
			String eventName = row.getString(1);
			String count = String.valueOf(row.getLong(2));
			System.out.println(eventId + "," + eventName + "," + count);
			
			if(Utils.isEmpty(eventId)) {
				continue;
			}
			
			JsonObject event = JsonObject.create().put("eventId", eventId).put("eventName", eventName).put("visitCount", count);
			events.add(event);
		}
		
		JsonObject totalEvents = JsonObject.create().put("totalEvents", JsonArray.from(events)); 
		couchbaseClint.insertFullStatisticsInfo(totalEvents);
		
		long t2 = System.currentTimeMillis();
		System.out.println("end full statistics ,use " + (t2-t1) + " ms");
	}
}
