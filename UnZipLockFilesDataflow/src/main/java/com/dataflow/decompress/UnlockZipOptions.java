package com.dataflow.decompress;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface UnlockZipOptions extends PipelineOptions {

	@Description("Bucket to contain zip files")
	String getSourceDirectory();
	void setSourceDirectory(String value);
	
	@Description("Path pattern of the files to read from")
	String getInputFilePattern();
	void setInputFilePattern(String value);

	@Description("Bucket to contain unlock zip files")
	@Validation.Required
	String getOutputDirectory();
	void setOutputDirectory(String value);

	@Description("Secret variable for extracting protected zip file")
	@Validation.Required
	String getSecretPasswordVariable();
	void setSecretPasswordVariable(String value);

	@Description("Stream buffer size")
	@Validation.Required
	@Default.String("1048576")
	String getStreamBufferSize();
	void setStreamBufferSize(String size);
	
	@Description("Location where fail files will move")
	@Validation.Required
	String getExceptionLocation();
	void setExceptionLocation(String value);


}
