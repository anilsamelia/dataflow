package com.dataflow.data.framework.bqUpload;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * BqUploadOptions class is user-define class that contains all the input
 * parameters.
 *
 * @author Anil.Kumar
 * @version 1.0
 * @since 2022-02-08
 */
public interface BqUploadOptions extends PipelineOptions {

	@Description("Source folder")
	@Default.String("gs://bucket_name/files/")
	String getSourceLocation();

	void setSourceLocation(String sourceLocation);

	@Description("After process completed location")
	@Default.String("gs://bucket_name/archive/")
	String getArchiveLocation();

	void setArchiveLocation(String archiveLocation);

	@Description("Fail-files' location")
	@Default.String("gs://bucket_name/exception/")
	String getExceptionLocation();

	void setExceptionLocation(String exceptionLocation);

	@Description("Files to Table mapping config file path")
	@Default.String("gs://bucket_name/global-config.csv")
	String getConfigFileLocation();

	void setConfigFileLocation(String configFileLocation);

}
