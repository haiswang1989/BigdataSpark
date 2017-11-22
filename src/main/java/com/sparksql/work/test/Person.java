package com.sparksql.work.test;

import java.io.Serializable;

/**
 * 对应Person内存表
 * @author haiswang
 *
 */
public class Person implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String id;
	
	private String name;
	
	private String sex;
	
	private int age;
	
	private long date;

	public long getDate() {
		return date;
	}

	public void setDate(long date) {
		this.date = date;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(id).append(",").append(name).append(",").append(sex).append(",").append(age).append(",").append(date);
		return sb.toString();
	}
}
