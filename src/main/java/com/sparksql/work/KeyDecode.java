package com.sparksql.work;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class KeyDecode implements Decoder<String> {
	
	public KeyDecode(VerifiableProperties verifiableProperties){  
		  
    }  
	
	public String fromBytes(byte[] arg0) {
		return new String(arg0);
	}
}