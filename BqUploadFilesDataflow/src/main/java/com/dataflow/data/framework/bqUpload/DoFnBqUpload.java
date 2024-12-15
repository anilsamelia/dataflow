package com.dataflow.data.framework.bqUpload;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.List;

/**
 * This class is a user-defined DoFn where its process the elements
 * 
 * @author Anil.Kumar
 * @version 1.0
 * @since 2022-02-08
 */

public class DoFnBqUpload extends DoFn<String, String> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(DoFnBqUpload.class);

	private static final String IN_PROGRESS = "_IN_PROGRESS";

	private List<JobMetaData> jobMetaDataList;
	private String sourceLocation;
	private String archiveLocation;
	private String exceptionLocation;

	public DoFnBqUpload(List<JobMetaData> jobMetaDataList, String sourceLocation, String archiveLocation,
			String exceptionLocation) {
		this.jobMetaDataList = jobMetaDataList;
		this.sourceLocation = sourceLocation;
		this.archiveLocation = archiveLocation;
		this.exceptionLocation = exceptionLocation;
	}

	@ProcessElement
	public void processElement(@Element String filePath, OutputReceiver<String> receiver) {

		ResourceId actualResource = FileSystems.matchNewResource(filePath, false);
		ResourceId inprogressResource = FileSystems.matchNewResource(filePath + IN_PROGRESS, false);
		try {
			FileSystems.rename(ImmutableList.of(actualResource), ImmutableList.of(inprogressResource),
					MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
		} catch (IOException e1) {
			LOG.error("URI not found file: " + filePath);
			e1.printStackTrace();
			receiver.output("No file found..file: " + filePath);
			return;
		}
		String fileName = filePath.replace(sourceLocation, "");
		String filePathInprogress = filePath + IN_PROGRESS;
		JobMetaData jobMetaData = extractMetadata(fileName);
		if (jobMetaData != null) {
			String datasetType = jobMetaData.getDatasetName();
			String tableName = jobMetaData.getTableName();
			String projectId = jobMetaData.getProjectId();
			String skipHeaderRow = String.valueOf(jobMetaData.getSkipheaderRow());
			String columnDelimiter = jobMetaData.getColumnDelimiter();
			String sourceBucketName = BqUploadUtility.getBucketName(filePathInprogress);
			String sourceObjectName = BqUploadUtility.getObjectName(filePathInprogress);
			String datasetName = determineDatasetName(datasetType);
			Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
			BigQuery bigQuery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
			if (null == storage.get(BlobId.of(sourceBucketName, sourceObjectName))) {
				LOG.error("URI not found file: " + filePath);
				receiver.output("No file found..file: " + filePath);
				return;
			}
			LOG.info("Starting bq upload of '" + filePath + "' file");
			LOG.info(String.format("File:" + filePath + " SourceBucket: %s", sourceBucketName));
			LOG.info(String.format("File:" + filePath + " TargetBucket: %s",
					BqUploadUtility.getBucketName(archiveLocation)));
			LOG.info(String.format("File:" + filePath + " DatasetName: %s", datasetName));
			LOG.info(String.format("File:" + filePath + " TableName: %s", tableName));
			String fileMoveToLocation = null;
			try {
				loadFileFromGCSToBigQuery(filePathInprogress, bigQuery, datasetName, tableName, skipHeaderRow,
						columnDelimiter);
				LOG.info(String.format("Bq successfully uploaded file: %s ", filePath));
				fileMoveToLocation = archiveLocation;
			} catch (Exception e) {
				LOG.error("Error file: " + filePath + " " + e.getMessage(), e);
				fileMoveToLocation = exceptionLocation;
			} finally {
				moveObject(inprogressResource, actualResource, fileMoveToLocation);
			}
		}
		LOG.info("Job completed file: " + filePath);
		receiver.output("Completed " + filePath);
	}

	/**
	 * moveObject method move file to different directory/bucket
	 * 
	 * @param ResourceId inprogressResource is renamed resource
	 * @param ResourceId actualResource is actual name for the resource
	 * @param String     moveToLocation location where the files are moved
	 */
	private void moveObject(ResourceId inprogressResource, ResourceId actualResource, String moveToLocation) {
		LOG.info("start move to " + moveToLocation + " file: " + inprogressResource);
		String inputFilename = inprogressResource.toString();
		String outputFilename = BqUploadUtility.getFileName(actualResource.toString());

		ResourceId outputDir = FileSystems.matchNewResource(moveToLocation, true);
		ResourceId outputFile = outputDir.resolve(outputFilename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

		try {
			FileSystems.copy(ImmutableList.of(inprogressResource), ImmutableList.of(outputFile),
					MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
			deleteFile(inprogressResource);
			LOG.info("File moved successfully file: " + actualResource);
		} catch (IOException e) {
			String msg = String.format("Error occurred while moving file %s to %s: %s", inputFilename, moveToLocation,
					e.getMessage());
			LOG.error(msg, e);
		}
	}

	/**
	 * deleteFile method used to delete the resource
	 * 
	 * @param ResourceId
	 */
	private void deleteFile(ResourceId inputFile) {
		try {
			FileSystems.delete(ImmutableList.of(inputFile), MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
		} catch (IOException e) {
			String msg = String.format("Error occurred while deleting file %s, the error is: %s", inputFile,
					e.getMessage());
			LOG.error(msg, e);
		}
	}

	/**
	 * determineDatasetName method is use create dynamic dataset based on input
	 * 
	 * @param String datasetType
	 */
	private String determineDatasetName(String datasetType) {
		String datasetName = null;
		LocalDate myLocal = LocalDate.now();
		int quarter = myLocal.get(IsoFields.QUARTER_OF_YEAR);
		int year = myLocal.getYear();
		String identifier = "_10p_q";
		if ("ckd".equals(datasetType)) {
			identifier = "_ckd_esrd_q";
		}
		datasetName = "raw_" + year + identifier + quarter;
		return datasetName;
	}

	/**
	 * loadFileFromGCSToBigQuery method is load the CSV file in big-query
	 * 
	 * @param String   filePath is gcs uri of the file
	 * @param BigQuery BigQuery
	 * @param String   DatasetName where data is insert.
	 * @param String   TableName is where data is insert in the table
	 * @param String   skipHeaderRow which use to skip the top lines of csv to be
	 *                 insert
	 * @param String   columnDelimiter is base on csv file delimiter.
	 */
	private void loadFileFromGCSToBigQuery(String filePath, BigQuery bigQuery, String datasetName, String tableName,
			String skipHeaderRow, String columnDelimiter) throws InterruptedException {
		String fileName = BqUploadUtility.getObjectName(filePath);
		CsvOptions.Builder csvBuilder = CsvOptions.newBuilder().setFieldDelimiter(columnDelimiter)
				.setSkipLeadingRows(Long.parseLong(skipHeaderRow));

		String quote = "\"";
		try {
			CsvOptions csvOption = csvBuilder.setQuote(quote).build();
			submitJob(csvOption, fileName, bigQuery, datasetName, tableName);
		} catch (BigQueryException e1) {
			try {
				quote = "^";
				CsvOptions csvOption = csvBuilder.setQuote(quote).build();
				submitJob(csvOption, fileName, bigQuery, datasetName, tableName);
			} catch (BigQueryException e2) {
				quote = "";
				CsvOptions csvOption = csvBuilder.setQuote(quote).build();
				submitJob(csvOption, fileName, bigQuery, datasetName, tableName);
			}
		}
	}

	/**
	 * submitJob method is submit the job in big-query
	 * 
	 * @param CsvOptions
	 * @param String     fileName
	 * @param BigQuery   bigQuery
	 * @param String     datasetName
	 * @param String     tableName
	 * 
	 */
	private void submitJob(CsvOptions csvOption, String fileName, BigQuery bigQuery, String datasetName,
			String tableName) throws InterruptedException, BigQueryException {
		LoadJobConfiguration loadConfig = LoadJobConfiguration
				.newBuilder(TableId.of(datasetName, tableName),
						"gs://" + BqUploadUtility.getBucketName(sourceLocation) + "/" + fileName, csvOption)
				.setAutodetect(false).build();
		Job job = bigQuery.create(JobInfo.of(loadConfig));
		job = job.waitFor();
		if (!job.isDone()) {
			String error = String.format(
					"gs://" + BqUploadUtility.getBucketName(sourceLocation) + "/" + fileName
							+ " Error bigQuery was unable to load into the table due to an error: %s",
					job.getStatus().getError());
			LOG.error(error);
			throw new RuntimeException(error);
		}
	}

	/**
	 * extractMetadata method is use to search job-meta-data using wildcardMatch
	 * 
	 * @param String filename
	 */
	private JobMetaData extractMetadata(String fileName) {
		for (JobMetaData job : jobMetaDataList) {
			if (FilenameUtils.wildcardMatch(fileName, job.getInputFilePattern())) {
				return job;
			}
		}
		return null;
	}

}
