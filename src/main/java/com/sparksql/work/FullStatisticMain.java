package com.sparksql.work;

import java.util.Timer;

import com.sparksql.work.constant.ConstInfo;

public class FullStatisticMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Timer timer = new Timer("JobTrackerTimer");
		timer.scheduleAtFixedRate(new FullStatisticsTask(), 0, ConstInfo.FULL_STATISTICS_PERIOD);
	}
}
