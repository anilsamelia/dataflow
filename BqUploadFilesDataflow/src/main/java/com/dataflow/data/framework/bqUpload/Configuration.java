package com.dataflow.data.framework.bqUpload;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/** 
* This class is a user-defined load the csv data into the list
* 
* @author  Anil.Kumar
* @version 1.0
* @since   2022-02-08 
*/
public class Configuration {

	StorageOptions options = StorageOptions.newBuilder().build();
	Storage storage = options.getService();

	private static final Logger logger = Logger.getLogger(Configuration.class.getName());
	
	/**
	 * CSV column names
	 */
	private enum CsvHeader {
		inputFilePattern, datasetName, tableName, projectId, skipheaderRow, columnDelimiter,
	}
	
	/**
	 * loadConfig method read the csv data and load into JobMetaData 
	 * @param String GCS mapping table URI
	 * @return List of JobMetaData
	 */
	public List<JobMetaData> loadConfig(String gcsFilePath) throws IOException {
		String bucketName = BqUploadUtility.getBucketName(gcsFilePath);
		String objectName = BqUploadUtility.getObjectName(gcsFilePath);
		Blob blob = storage.get(bucketName, objectName);
		ReadChannel readChannel = blob.reader();
		CSVParser csvParser = null;
		JobMetaData jobmetaData = null;
		BufferedReader reader = null;
		List<JobMetaData> metadatalist = null;
		try {
			reader = new BufferedReader(Channels.newReader(readChannel, String.valueOf(StandardCharsets.UTF_8)));
			metadatalist = new ArrayList<JobMetaData>();
			csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader(CsvHeader.class));
			for (CSVRecord csvRecord : csvParser) {
				if (csvRecord.get(CsvHeader.skipheaderRow).equals(CsvHeader.skipheaderRow.toString())) {
					continue; // skip the header line
				}
				jobmetaData = new JobMetaData();
				jobmetaData.setProjectId(csvRecord.get(CsvHeader.projectId));
				jobmetaData.setDatasetName(csvRecord.get(CsvHeader.datasetName));
				jobmetaData.setTableName(csvRecord.get(CsvHeader.tableName));
				jobmetaData.setInputFilePattern(csvRecord.get(CsvHeader.inputFilePattern));
				jobmetaData.setSkipheaderRow(Integer.parseInt(csvRecord.get(CsvHeader.skipheaderRow)));
				jobmetaData.setColumnDelimiter(csvRecord.get(CsvHeader.columnDelimiter));
				metadatalist.add(jobmetaData);
			}
			logger.info(gcsFilePath +" Total Rows: "+metadatalist.size());
			logger.info(gcsFilePath +" configuration is loaded successfully");
		} catch (FileNotFoundException e1) {
			logger.log(Level.SEVERE, String.format("Error while loading configuration: %s", gcsFilePath));
		} finally {
			try {
				if (csvParser != null) {
					csvParser.close();
				}
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				logger.log(Level.SEVERE, "Error while parser close");
			}
			return metadatalist;
		}
	}
	

}
