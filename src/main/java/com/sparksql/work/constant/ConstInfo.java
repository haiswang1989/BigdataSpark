package com.sparksql.work.constant;

/**
 * 常量信息
 * @author haiswang
 *
 */
public class ConstInfo {
	
	public static final String SPARK_APP_NAME = "SparkSQLKafka";
	public static final String SPARK_MASTER = "local[1]";
	
	public static final String KAFKA_BROKERS = "chao-zk1-3546.slc01.dev.ebayc3.com:9092,chao-zk2-6549.slc01.dev.ebayc3.com:9092,chao-zk3-3574.slc01.dev.ebayc3.com:9092";
	public static final String ZOOKEEPER_BROKERS = "chao-zk1-3546.slc01.dev.ebayc3.com:2181,chao-zk2-6549.slc01.dev.ebayc3.com:2181,chao-zk3-3574.slc01.dev.ebayc3.com:2181/kafka";
	public static final String KAFKA_MESSAGE_TOPIC = "tracking_events";
	
	//KAFKA拉取数据时间间隔(统计时间窗口大小)
	public static final long KAFKA_PULL_MESSAGE_PERIOD = 10000l;
	
	//落地数据HDFS目录
	public static final String HDFS_DATA_PATH = "hdfs://topgun-spark1-8367.lvs01.dev.ebayc3.com:9000/data/spark/trackevent/";
	
	//增量统计时间间隔,建议和KAFKA_PULL_MESSAGE_PERIOD保持一致,这样可以在触发拉取时进行增量统计
	public static final long INCREMENT_STATISTICS_PERIOD = KAFKA_PULL_MESSAGE_PERIOD;
	
	//全量统计时间间隔(10分钟)
	public static final long FULL_STATISTICS_PERIOD = 10 * 60 * 1000;
	
	//内存表的名称(全量数据)
	public static final String MT_USER_TABLE_FULL = "MT_EVENT_FULL";
	public static final String MT_USER_TABLE_INCREMENT = "MT_EVENT_INCREMENT";
	
	public static final String HQL_INCREMENT_STATISTICS = "select genreId,genre,count(*) cnt from " + ConstInfo.MT_USER_TABLE_INCREMENT + " group by genre,genreId";
	public static final String HQL_FULL_STATISTICS = "select genreId,genre,count(*) cnt from " + ConstInfo.MT_USER_TABLE_FULL + " group by genre,genreId";
	
	//HDFS文件更新时间间隔
	public static final int CHANGE_HDFS_FILE_PERIOD = 5 * 60 * 1000;  
}
