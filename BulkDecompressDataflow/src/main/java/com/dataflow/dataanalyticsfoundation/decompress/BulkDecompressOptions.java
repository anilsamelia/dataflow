package com.dataflow.dataanalyticsfoundation.decompress;

import org.apache.beam.sdk.options.*;

/**
 * BulkDecompressOptions class is user-define class that contains all the input
 * parameters.
 *
 * @author Barkat.Dhillon
 * @version 1.0
 * @since 2022-02-08
 */
public interface BulkDecompressOptions extends PipelineOptions {

	@Description("The input file pattern to read from (e.g. gs://bucket-name/compressed/*.gz)")
	@Validation.Required
	@Default.String("gs://bucket_name/landing/*.gz")
	ValueProvider<String> getInputFilePattern();

	void setInputFilePattern(ValueProvider<String> value);

	@Description("The output location to write to (e.g. gs://bucket-name/decompressed/)")
	@Validation.Required
	@Default.String("gs://bucket_name/decompressed/")
	ValueProvider<String> getOutputDirectory();

	void setOutputDirectory(ValueProvider<String> value);

	@Description("The fail location to write files which were not decompressed (e.g. gs://bucket-name/error/)")
	@Default.String("gs://bucket_name/error/decompress/")
	ValueProvider<String> getErrorDirectory();

	void setErrorDirectory(ValueProvider<String> errorDirectory);

}
