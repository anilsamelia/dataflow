package com.dataflow.data.framework.bqUpload;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * BqUploadPipeline is main class where Data-flow start to execute.
 * 
 * 
 * @author Anil.Kumar
 * @version 1.0
 * @since 2022-02-08
 */

public class BqUploadPipeline {

	/**
	 * The main entry-point for pipeline execution.
	 */
	static void runBqUploadFiles(BqUploadOptions options) throws PipelineExecutionException, IOException {
		String configFileLocation = options.getConfigFileLocation();
		Configuration config = new Configuration();

		List<JobMetaData> configData = config.loadConfig(configFileLocation);
		List<String> filePatterns = loadFilePatterns(configData, options.getSourceLocation());

		Pipeline p = Pipeline.create(options);
		p.apply(Create.of(filePatterns)).apply(FileIO.matchAll()).apply("Reading matching files", FileIO.readMatches())
				.apply("Create PCollection",
						MapElements.into(TypeDescriptors.strings()).via((FileIO.ReadableFile file) -> {
							String fileName = file.getMetadata().resourceId().toString();
							return fileName;
						}))
				.apply("BQ Upload", new ParDoBqUpload(configData, options.getSourceLocation(),
						options.getArchiveLocation(), options.getExceptionLocation()));
		p.run();
	}

	private static List<String> loadFilePatterns(List<JobMetaData> config, String sourceFolder) {
		List<String> list = new ArrayList<>();
		for (JobMetaData jobData : config) {
			list.add(sourceFolder + jobData.getInputFilePattern());
		}
		return list;
	}

	public static void main(String[] args) throws IOException {
		BqUploadOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BqUploadOptions.class);
		runBqUploadFiles(options);
	}
}
