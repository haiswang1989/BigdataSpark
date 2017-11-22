package com.sparksql.work;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.alibaba.fastjson.JSON;
import com.sparksql.work.bean.TEvent;
import com.sparksql.work.bean.TEvents;
import com.sparksql.work.constant.ConstInfo;
import com.sparksql.work.util.BizUtils;
import com.sparksql.work.util.Utils;

/**
 * 从Kafka接实时数据存入内存表用于统计
 * @author haiswang
 *
 */
public class SparkSQLKafka implements Serializable {

	private static final long serialVersionUID = 2140676322100834336L;
	
	//hadoop filesystem
	private transient FileSystem fileSystem = null;
	
	private DataFrame incrementDataFrame = null;
	
	
	transient SparkConf sparkConf = new SparkConf().setAppName(ConstInfo.SPARK_APP_NAME).setMaster(ConstInfo.SPARK_MASTER);
	transient JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
	transient JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, new Duration(ConstInfo.KAFKA_PULL_MESSAGE_PERIOD));
	transient SQLContext sqlContext = new SQLContext(javaSparkContext);
	
	private CouchbaseClient couchbaseClient = new CouchbaseClient();
	
	public JavaSparkContext getJavaSparkContext() {
		return javaSparkContext;
	}
	
	public SQLContext getSqlContext() {
		return sqlContext;
	}
	
	public JavaStreamingContext getJavaStreamingContext() {
		return javaStreamingContext;
	}
	
	/**
	 * 初始化FileSystem对象
	 * @return
	 * @throws IOException 
	 */
	public void initFileSystem() throws IOException {
		Configuration configuration = new Configuration();
		fileSystem = FileSystem.get(configuration);
	}
	
	/**
	 * 增量数据内存表
	 * @param userData
	 */
	public void loadToMemoryTable(JavaRDD<TEvent> userData) {
		incrementDataFrame = sqlContext.createDataFrame(userData, TEvent.class);
		incrementDataFrame.registerTempTable(ConstInfo.MT_USER_TABLE_INCREMENT);
	}
		
	/**
	 * 启动前的初始化
	 * @return
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public void init() throws IOException {
		initFileSystem();
		HdfsClient.getInstance().init(fileSystem);
	}
	
	public JavaDStream<TEvents> getKafkaStream() {
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("auto.offset.reset", "largest");
		kafkaParams.put("zookeeper.connect", ConstInfo.ZOOKEEPER_BROKERS);
		kafkaParams.put("group.id", "haiswang");
		
		Set<String> topics = new HashSet<>();
		topics.add(ConstInfo.KAFKA_MESSAGE_TOPIC);
		return BizUtils.getKafkaDStream(javaStreamingContext, kafkaParams, topics);
	}
	
	@SuppressWarnings("serial")
	public void start() {
		
		JavaDStream<TEvents> lineValue = getKafkaStream();
		
		JavaDStream<TEvent> singleEvent = lineValue.flatMap(new FlatMapFunction<TEvents, TEvent>() {
			@Override
			public Iterable<TEvent> call(TEvents tEvents) throws Exception {
				return tEvents.gettEvents();
			}
		});
		
		//数据清洗
		singleEvent = singleEvent.filter(new Function<TEvent, Boolean>() {
			@Override
			public Boolean call(TEvent tEvent) throws Exception {
				return "1".equals(tEvent.getCoreProductView());
			}
		});
		
		singleEvent.foreachRDD(new VoidFunction<JavaRDD<TEvent>>() {
			@Override
			public void call(JavaRDD<TEvent> tEvent) throws Exception {
				//触发拉取
				int recordSize = tEvent.collect().size();
				if(0 != recordSize) {
					//这边可以提出去
					for (TEvent te : tEvent.collect()) {
						String jsonText = JSON.toJSONString(te);
						System.out.println(jsonText);
						HdfsClient.getInstance().writeLine(jsonText);
					}
					
					System.out.println("load to memory table ...");
					//实时从Kafka接入的数据加入历史数据库
					loadToMemoryTable(tEvent);
					//触发增量统计
					doIncrementStatistics();
					System.out.println("load to memory table end...");
				} else {
					//增量全部是0
				}
			}
		});
	}
	
	/**
	 * 增量统计
	 */
	public void doIncrementStatistics() {
		System.out.println("增量统计---------------------");
		
		DataFrame resultDataFrame = sqlContext.sql(ConstInfo.HQL_INCREMENT_STATISTICS);
		
		Map<String, Map<String, String>> metaData = new HashMap<>();
		
		for (Row row : resultDataFrame.collect()) {
			String eventId = row.getString(0);
			String eventName = row.getString(1);
			String count = String.valueOf(row.getLong(2));
			System.out.println(eventId + "," + eventName + "," + count);
			
			if(Utils.isEmpty(eventId)) {
				continue;
			}
			
			Map<String, String> partInfo = new HashMap<>();
			partInfo.put("eventName", eventName);
			partInfo.put("count", count);
			metaData.put(eventId, partInfo);
		}
		
		if(0 == metaData.size()) {
			return;
		}
		
		couchbaseClient.insertDataFromCouchBaseByDocumentName(metaData);
		
		System.out.println("-----------------------------");
	}
	
	public static void main(String[] args) {
		
		SparkSQLKafka sparkSQLKafka = new SparkSQLKafka();
		
		try {
			sparkSQLKafka.init();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		sparkSQLKafka.start();
		
		JavaStreamingContext streamContext = sparkSQLKafka.getJavaStreamingContext();
		
		streamContext.start();
		streamContext.awaitTermination();
	}
}


