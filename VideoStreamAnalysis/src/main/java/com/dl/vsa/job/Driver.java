package com.dl.vsa.job;

import org.apache.log4j.Logger;

import com.dl.vsa.spark.VideoStreamProcessor;

public class Driver {

	private static final Logger logger = Logger.getLogger(Driver.class);

	public static void main(String[] args) {
		logger.info("Starting stream processor");
		try {
			VideoStreamProcessor.startVideoProcessor();
		} catch (Exception e) {
			logger.error("Some error occured while processing video");
			e.printStackTrace();
		}
	}

}
