package com.sparksql.work.util;

import java.text.SimpleDateFormat;
import java.util.Date;


public class Utils {
	
	public static String getHost(String referer) {
//		if(null == referer || "".equals(referer.trim())) {
//			return "";
//		}
//		
//		return referer.substring(8 ,referer.indexOf(".com") + 4);
		return "";
	}
	
	public static String getGenreIdDetail(String referer) {
//		String returnVal = referer.substring(referer.indexOf("com/") + 4);
//		returnVal = returnVal.substring(0, returnVal.indexOf("/"));
//		return returnVal;
		return "";
	}
	
	public static long formatTime(String time) {
//		SimpleDateFormat sdf = new SimpleDateFormat(ConstInfo.TIME_FORMAT);
//		Date date = null;
//		try {
//			date = sdf.parse(time);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		
//		return null == date ? 0l : date.getTime();
		return System.currentTimeMillis();
	}
	
	/**
	 * 获取当前的时间
	 * @return
	 */
	public static String getCurrDate() {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String currDate = sdf.format(new Date());
		return currDate;
	}
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	public static boolean isEmpty(String input) {
		if(null == input || "null".equals(input)) {
			return true;
		}
		
		return false;
	}
	
	public static void main(String[] args) {
		System.out.println(getCurrDate());
	}
}
