package com.sparksql.work;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.sparksql.work.constant.ConstInfo;
import com.sparksql.work.util.Utils;

/**
 * 
 * @author haiswang
 *
 */
public class HdfsClient implements Serializable {
	
	private static final long serialVersionUID = 1L;

	private static volatile HdfsClient instance = null;
	
	private FSDataOutputStream fsDos = null;
	
	private BufferedWriter bw = null;
	
	private FileSystem fileSystem = null;
	
	private HdfsClient() {}
	
	public static HdfsClient getInstance() {
		if(null == instance) {
			synchronized (HdfsClient.class) {
				if(null == instance) {
					instance = new HdfsClient();
				}
			}
		}
		
		return instance;
	}
	
	/**
	 * 
	 * @param fileSystem
	 * @throws IOException
	 */
	public void init(FileSystem fileSystem) throws IOException {
		this.fileSystem = fileSystem; 
		createFileIfNotExist();
		
		Timer timer = new Timer("ControlTimer");
		timer.scheduleAtFixedRate(new ControlThread(this), ConstInfo.CHANGE_HDFS_FILE_PERIOD, ConstInfo.CHANGE_HDFS_FILE_PERIOD);
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	public void createFileIfNotExist() throws IOException {
		String currDate = Utils.getCurrDate();
		Path createFile = new Path(ConstInfo.HDFS_DATA_PATH + "trackevent_" + currDate + ".nb");
		
		if(fileSystem.exists(createFile)) { //file exists ,do append
			fsDos = fileSystem.append(createFile);
			bw = new BufferedWriter(new OutputStreamWriter(fsDos));
		} else { //file not exists ,do create
			fsDos = fileSystem.create(createFile);
			bw = new BufferedWriter(new OutputStreamWriter(fsDos));
		}
	}
	
	public synchronized void writeLine(String text) throws IOException {
		bw.write(text);
		bw.newLine();
	}
	
	public synchronized void switchHdfsFile() {
		
		try {
			bw.flush();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		close();
		
		try {
			createFileIfNotExist();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * close I/O
	 */
	public void close() {
		IOUtils.closeStream(fsDos);
	}
}

class ControlThread extends TimerTask {
	
	private HdfsClient hdfsClient;
	
	public ControlThread(HdfsClient hdfsClient) {
		this.hdfsClient = hdfsClient;
	}

	@Override
	public void run() {
		System.out.println("swich hdfs file....");
		hdfsClient.switchHdfsFile();
	}
}
