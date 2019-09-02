package com.dl.vsc.job;

import org.apache.log4j.Logger;

import com.dl.vsc.kafka.VideoStreamCollector;

public class Driver {

	private static final Logger logger = Logger.getLogger(Driver.class);

	public static void main(String[] args) {
		try {
			logger.info("Starting the video stream collector");
			VideoStreamCollector.startVideosCollection();
		} catch (Exception e) {
			logger.error("Some error occured while starting videos collection");
			e.printStackTrace();
		}

	}

}
