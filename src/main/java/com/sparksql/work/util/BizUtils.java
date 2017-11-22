package com.sparksql.work.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.sparksql.work.ValueDecode;
import com.sparksql.work.bean.TEvent;
import com.sparksql.work.bean.TEvents;
import com.sparksql.work.bean.TrackingEvent;
import com.sparksql.work.bean.TrackingEvents;
import com.sparksql.work.constant.ConstInfo;

public class BizUtils {
	
	@SuppressWarnings("serial")
	public static JavaDStream<TEvents> getKafkaDStream(JavaStreamingContext javaStreamingContext ,Map<String, String> kafkaParams ,Set<String> topics) {
		
		//JavaPairInputDStream<String, TrackingEvents> messages = KafkaUtils.createDirectStream(javaStreamingContext, String.class, TrackingEvents.class, StringDecoder.class, ValueDecode.class, kafkaParams, topics);
		
		Map<String, Integer> topic = new HashMap<String, Integer>();
		topic.put(ConstInfo.KAFKA_MESSAGE_TOPIC, 8);
		
		JavaPairInputDStream<String, TrackingEvents> messages = KafkaUtils.createStream(javaStreamingContext,String.class, TrackingEvents.class, StringDecoder.class, ValueDecode.class ,kafkaParams , topic ,StorageLevel.MEMORY_AND_DISK());
		
		JavaDStream<TEvents> values = messages.map(new Function<Tuple2<String,TrackingEvents>, TEvents>() {
			public TEvents call(Tuple2<String,TrackingEvents> v1) throws Exception {
				TrackingEvents events = v1._2;
				
				TEvents tEvents = new TEvents();
				TEvent event = null;
				Map<String, String> context = events.getContext();
				String userAgent = context.get("userAgent");
				String ipAddress = context.get("ipaddress");
				String serverTimestamp = context.get("serverTimestamp");
				String visitorId = context.get("visitorId");
				String userId = context.get("userId");
				String referer = context.get("referer");
				
				String host = Utils.getHost(referer);
				String genreIdDetail = Utils.getGenreIdDetail(referer);
				
				String pageName = null;
				String siteSections = null;
				String coreProductView = null; 
				String category = null;
				String subCategory = null;
				String genre = null;
				String genreId = null;
				String products = null;
				
				
				for (TrackingEvent trackingEvent : events.getEvents()) {
					Map<String, String> properties = trackingEvent.getProperties();
					event = new TEvent();
					pageName = properties.get("pageName");
					siteSections = properties.get("siteSections");
					coreProductView = properties.get("core:productView");
					category = properties.get("category");
					subCategory = properties.get("subCategory");
					genre = properties.get("genre");
					genreId = properties.get("genreId");
					products = properties.get("products");
					event.setUserAgent(userAgent);
					event.setIpaddress(ipAddress);
					event.setServerTimestamp(Utils.formatTime(serverTimestamp));
					event.setVisitorId(visitorId);
					event.setUserId(userId);
					event.setReferer(referer);
					event.setHost(host);
					event.setGenreIdDetail(genreIdDetail);
					event.setPageName(pageName);
					event.setSiteSections(siteSections);
					event.setCoreProductView(coreProductView);
					event.setCategory(category);
					event.setSubCategory(subCategory);
					event.setGenre(genre);
					event.setGenreId(genreId);
					event.setProducts(products);
					tEvents.add(event);
				}
				
				return tEvents;
			}
		});
		
		return values;
	}
	
}
