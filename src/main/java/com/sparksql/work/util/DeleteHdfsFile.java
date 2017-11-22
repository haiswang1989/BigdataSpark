package com.sparksql.work.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteHdfsFile {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration config = new Configuration();
		FileSystem fileSystem = FileSystem.get(config);
		
		Path path = new Path("hdfs://topgun-spark1-8367.lvs01.dev.ebayc3.com:9000/datacenter");
		
		FileStatus[] filesStatus = fileSystem.listStatus(path);
		for (FileStatus fileStatus : filesStatus) {
			
			fileSystem.delete(fileStatus.getPath(), true);
		}
		
		System.out.println("over...");
	}

}
