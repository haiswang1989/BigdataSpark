package com.sparksql.work;

import java.io.Serializable;

import com.google.gson.Gson;
import com.sparksql.work.bean.TrackingEvents;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class ValueDecode implements Decoder<TrackingEvents> ,Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8042565531457633889L;
	transient private Gson gson = new Gson();
	
	@Override
	public TrackingEvents fromBytes(byte[] arg0) {
		return gson.fromJson(new String(arg0), TrackingEvents.class);
	}
	
    public ValueDecode(VerifiableProperties verifiableProperties){  
  
    }  
}
