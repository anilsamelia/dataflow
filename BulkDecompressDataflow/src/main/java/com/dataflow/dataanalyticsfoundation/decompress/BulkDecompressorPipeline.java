package com.dataflow.dataanalyticsfoundation.decompress;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BulkDecompressorPipeline is main class where Dataflow start to execute.
 * 
 * @author Barkat.Dhillon
 * @version 1.0
 * @since 2022-02-08
 */
public class BulkDecompressorPipeline {

    /**
     * The logger to output status messages to.
     */
    private static final Logger LOG = LoggerFactory.getLogger(BulkDecompressorPipeline.class);

    /**
     * The main entry-point for pipeline execution. This method will start the pipeline but will not
     * wait for it's execution to finish. If blocking execution is required, use the {@link
     * BulkDecompressorPipeline#run(BulkDecompressOptions)} method to start the pipeline and invoke {@code
     * result.waitUntilFinish()} on the {@link PipelineResult}.
     *
     * @param args The command-line args passed by the executor.
     */
    public static void main(String[] args) {

        BulkDecompressOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BulkDecompressOptions.class);

        run(options);
    }

    /**
     * Runs the pipeline to completion with the specified options. This method does not wait until the
     * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
     * object to block until the pipeline is finished running if blocking programmatic execution is
     * required.
     *
     * @param options The execution options.
     * @return The pipeline result.
     */
    public static PipelineResult run(BulkDecompressOptions options) {

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        LOG.info("Starting decompress pipeline...");
        // Run the pipeline over the work items.
        pipeline
                .apply("MatchFile(s)", FileIO.match().filepattern(options.getInputFilePattern()))
                .apply(
                        "DecompressFile(s)",
                        ParDo.of(new DoFnDecompress(options.getOutputDirectory(), options.getErrorDirectory())));

        return pipeline.run();
    }
}
