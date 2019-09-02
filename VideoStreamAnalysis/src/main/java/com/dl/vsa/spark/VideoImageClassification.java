package com.dl.vsa.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;
import org.tensorflow.Tensor;

import com.dl.vsa.ml.LabelImage;
import com.dl.vsa.utils.VideoEventData;

public class VideoImageClassification implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(VideoImageClassification.class);

	static {
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}

	public static VideoEventData classifyImage(String camId, Iterator<VideoEventData> frames, String outputDir,
			VideoEventData previousProcessedEventData, byte[] graphDef, List<String> labels) throws Exception {
		VideoEventData currentProcessedEventData = new VideoEventData();
		Mat frame = null;
		Mat grayFrame = null;

		if (previousProcessedEventData != null) {
			logger.warn(
					"cameraId=" + camId + " previous processed timestamp=" + previousProcessedEventData.getTimestamp());
			Mat preFrame = getMat(previousProcessedEventData);
			Mat preGrayFrame = new Mat(preFrame.size(), CvType.CV_8UC1);
			Imgproc.cvtColor(preFrame, preGrayFrame, Imgproc.COLOR_BGR2GRAY);
			Imgproc.GaussianBlur(preGrayFrame, preGrayFrame, new Size(3, 3), 0);
		}

		ArrayList<VideoEventData> sortedList = new ArrayList<VideoEventData>();
		while (frames.hasNext()) {
			sortedList.add(frames.next());
		}
		sortedList.sort(Comparator.comparing(VideoEventData::getTimestamp));
		logger.warn("cameraId=" + camId + " total frames=" + sortedList.size());

		for (VideoEventData eventData : sortedList) {
			frame = getMat(eventData);
			grayFrame = new Mat(frame.size(), CvType.CV_8UC1);
			Imgproc.cvtColor(frame, grayFrame, Imgproc.COLOR_BGR2GRAY);
			Imgproc.GaussianBlur(grayFrame, grayFrame, new Size(3, 3), 0);
			logger.warn("cameraId=" + camId + " timestamp=" + eventData.getTimestamp());
			currentProcessedEventData = eventData;

			byte[] imageBytes = Base64.getDecoder().decode(eventData.getData());
			try (Tensor<Float> image = LabelImage.constructAndExecuteGraphToNormalizeImage(imageBytes)) {
				float[] labelProbabilities = LabelImage.executeInceptionGraph(graphDef, image);
				int bestLabelIdx = LabelImage.maxIndex(labelProbabilities);
				logger.info(String.format("BEST MATCH: %s (%.2f%% likely)", labels.get(bestLabelIdx),
						labelProbabilities[bestLabelIdx] * 100f));
			}

		}
		return currentProcessedEventData;
	}

	private static Mat getMat(VideoEventData ed) throws Exception {
		Mat mat = new Mat(ed.getRows(), ed.getCols(), ed.getType());
		mat.put(0, 0, Base64.getDecoder().decode(ed.getData()));
		return mat;
	}

}