package com.sparksql.work;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * Couchbase client
 * @author haiswang
 *
 */
public class CouchbaseClient implements Serializable {

	private static final long serialVersionUID = 8798225014641439569L;
	
	private static transient List<String> nodes = new ArrayList<String>();
	
	static {
		nodes.add("10.65.244.61");
		nodes.add("10.65.244.64");
	}
	
	private String documentName = "meta";
	
	private static final String CB_REAL_TOTAL_COUNT = "REAL_TOTAL_COUNT";
	
	private String username = "common";
	private String password = "";
	
	private transient CouchbaseEnvironment env = null;
	private transient CouchbaseCluster cluster = null;
	private transient Bucket bucket = null;
	
	public CouchbaseClient() {
		env = DefaultCouchbaseEnvironment.builder().connectTimeout(10000l).requestBufferSize(1024).build();
		cluster = CouchbaseCluster.create(env, nodes);
		bucket = cluster.openBucket(username, password);
	}
	
	/**
	 * 
	 * @param eventId
	 * @param eventName
	 * @param count
	 */
	public void insertDataFromCouchBaseByDocumentName(String eventId ,String eventName ,String count) {
	    Map<String, Object> metaData = new HashMap<>();
	    metaData.put("eventId", eventId);
	    metaData.put("eventName", eventName);
	    metaData.put("count", count);
	    JsonObject opMetaData = JsonObject.from(metaData);
	    bucket.upsert(JsonDocument.create(documentName, opMetaData));
	}
	
	/**
	 * 
	 * @param metaData
	 */
	public void insertDataFromCouchBaseByDocumentName(Map<String, Map<String, String>> metaData) {
		JsonObject opMetaData = JsonObject.from(metaData);
	    bucket.upsert(JsonDocument.create(documentName, opMetaData));
	}
	
	/**
	 * 
	 * @param totalEvents
	 */
	public void insertFullStatisticsInfo(JsonObject totalEvents) {
	    bucket.upsert(JsonDocument.create(CB_REAL_TOTAL_COUNT, totalEvents));
	}
	
	/**
	 * 
	 * @param documentName
	 * @return
	 */
	public JsonDocument getDataFromCouchBaseByDocumentName() {
	    JsonDocument jsonDocument = bucket.get(documentName);
	    return jsonDocument;
	}

	/**
	 * 
	 */
	public void close() {
		cluster.disconnect();
		env.shutdown();
	}
}
