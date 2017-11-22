package com.sparksql.work.test;

/**
 * 对应内存表(职业表)
 * @author haiswang
 *
 */
public class Occupation {
	
	//内存表person的ID
	private String id;
	
	//职业的名字
	private String occupationName;
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getOccupationName() {
		return occupationName;
	}
	
	public void setOccupationName(String occupationName) {
		this.occupationName = occupationName;
	}
}
