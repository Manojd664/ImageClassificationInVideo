package com.dl.vsa.spark;

import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.dl.vsa.ml.LabelImage;
import com.dl.vsa.utils.PropertyFileReader;
import com.dl.vsa.utils.VideoEventData;

public class VideoStreamProcessor {
	private static final Logger logger = Logger.getLogger(VideoStreamProcessor.class);

	public static void startVideoProcessor() throws Exception {

		Properties prop = PropertyFileReader.readPropertyFile();

		byte[] graphDef = LabelImage.readAllBytesOrExit(
				Paths.get(prop.getProperty("tensorflow.model.dir"), "tensorflow_inception_graph.pb"));

		List<String> labels = LabelImage.readAllLinesOrExit(
				Paths.get(prop.getProperty("tensorflow.model.dir"), "imagenet_comp_graph_label_strings.txt"));

		SparkSession spark = SparkSession.builder().appName("VideoStreamProcessor")
				.master(prop.getProperty("spark.master.url")).getOrCreate();

		final String processedImageDir = prop.getProperty("processed.output.dir");
		logger.warn("Output directory for saving processed images is set to " + processedImageDir
				+ ". This is configured in processed.output.dir key of property file.");

		StructType schema = DataTypes.createStructType(
				new StructField[] { DataTypes.createStructField("cameraId", DataTypes.StringType, true),
						DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
						DataTypes.createStructField("rows", DataTypes.IntegerType, true),
						DataTypes.createStructField("cols", DataTypes.IntegerType, true),
						DataTypes.createStructField("type", DataTypes.IntegerType, true),
						DataTypes.createStructField("data", DataTypes.StringType, true) });

		// Create DataSet from stream messages from kafka
		Dataset<VideoEventData> ds = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
				.option("subscribe", prop.getProperty("kafka.topic"))
				.option("kafka.max.partition.fetch.bytes", prop.getProperty("kafka.max.partition.fetch.bytes"))
				.option("kafka.max.poll.records", prop.getProperty("kafka.max.poll.records")).load()
				.selectExpr("CAST(value AS STRING) as message")
				.select(functions.from_json(functions.col("message"), schema).as("json")).select("json.*")
				.as(Encoders.bean(VideoEventData.class));

		// key-value pair of cameraId-VideoEventData
		KeyValueGroupedDataset<String, VideoEventData> kvDataset = ds
				.groupByKey(new MapFunction<VideoEventData, String>() {
					@Override
					public String call(VideoEventData value) throws Exception {
						return value.getCameraId();
					}
				}, Encoders.STRING());

		// process
		Dataset<VideoEventData> processedDataset = kvDataset.mapGroupsWithState(
				new MapGroupsWithStateFunction<String, VideoEventData, VideoEventData, VideoEventData>() {
					@Override
					public VideoEventData call(String key, Iterator<VideoEventData> values,
							GroupState<VideoEventData> state) throws Exception {
						logger.warn("CameraId=" + key + " PartitionId=" + TaskContext.getPartitionId());
						VideoEventData existing = null;
						if (state.exists()) {
							existing = state.get();
						}
						VideoEventData processed = VideoImageClassification.classifyImage(key, values,
								processedImageDir, existing, graphDef, labels);

						if (processed != null) {
							state.update(processed);
						}
						return processed;
					}
				}, Encoders.bean(VideoEventData.class), Encoders.bean(VideoEventData.class));

		StreamingQuery query = processedDataset.writeStream().outputMode("update").format("console").start();

		query.awaitTermination();
	}
}