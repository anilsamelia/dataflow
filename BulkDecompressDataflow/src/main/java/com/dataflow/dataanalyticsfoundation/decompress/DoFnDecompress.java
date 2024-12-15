package com.dataflow.dataanalyticsfoundation.decompress;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Performs the decompression of an object on Google Cloud Storage and uploads
 * the decompressed object back to a specified destination location.
 */
public class DoFnDecompress extends DoFn<MatchResult.Metadata, String> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(DoFnDecompress.class);

	/**
	 * A list of the {@link Compression} values excluding {@link Compression#AUTO}
	 * and {@link Compression#UNCOMPRESSED}.
	 */
	static final Set<Compression> SUPPORTED_COMPRESSIONS = Stream.of(Compression.values())
			.filter(value -> value != Compression.AUTO && value != Compression.UNCOMPRESSED)
			.collect(Collectors.toSet());

	static final String UNCOMPRESSED_ERROR_MSG = "Skipping file %s because it did not match any compression mode (%s)";

	static final String MALFORMED_ERROR_MSG = "The file resource %s is malformed or not in %s compressed format.";
	static final TupleTag<KV<String, String>> DEADLETTER_TAG = new TupleTag<KV<String, String>>() {
	};

	private static final String GCS_PATH_PREFIX = "gs://";
	private ValueProvider<String> destinationLocation;
	private ValueProvider<String> errorLocation;

	public DoFnDecompress(ValueProvider<String> destinationLocation, ValueProvider<String> errorLocation) {
		this.destinationLocation = destinationLocation;
		this.errorLocation = errorLocation;
	}

	@ProcessElement
	public void processElement(ProcessContext context) {
		ResourceId inputFile = context.element().resourceId();

		if (!Compression.AUTO.isCompressed(inputFile.toString())) {
			String errorMsg = String.format(UNCOMPRESSED_ERROR_MSG, inputFile.toString(), SUPPORTED_COMPRESSIONS);
			LOG.error(errorMsg);
			moveToErrorFolder(inputFile);
		} else {
			try {
				ResourceId outputFile = decompress(inputFile);
				LOG.info(String.format(
						"File %s is decompressed successfully, now removing this file from source folder.", inputFile));
				deleteFile(inputFile);
				context.output(outputFile.toString());
			} catch (IOException e) {
				LOG.error(e.getMessage());
				moveToErrorFolder(inputFile);
			}
		}
	}

	/**
	 * Decompresses the inputFile using the specified compression and outputs to the
	 * main output of the {@link DoFnDecompress} doFn. Files output to the
	 * destination will be first written as temp files with a "temp-" prefix within
	 * the output directory.
	 *
	 * @param inputFile The inputFile to decompress.
	 * @return A {@link ResourceId} which points to the resulting file from the
	 *         decompression.
	 */
	private ResourceId decompress(ResourceId inputFile) throws IOException {
		// Remove the compressed extension from the file. Example: demo.txt.gz ->
		// demo.txt
		String outputFilename = Files.getNameWithoutExtension(inputFile.toString());

		// Resolve the necessary resources to perform the transfer.
		ResourceId outputDir = FileSystems.matchNewResource(destinationLocation.get(), true);
		ResourceId outputFile = outputDir.resolve(outputFilename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
		ResourceId tempFile = outputDir.resolve(
				Files.getFileExtension(inputFile.toString()) + "-temp-" + outputFilename,
				ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

		// Resolve the compression
		Compression compression = Compression.detect(inputFile.toString());

		// Perform the copy of the decompressed channel into the destination.
		try (ReadableByteChannel readerChannel = compression.readDecompressed(FileSystems.open(inputFile))) {
			try (WritableByteChannel writerChannel = FileSystems.create(tempFile, MimeTypes.TEXT)) {
				ByteStreams.copy(readerChannel, writerChannel);
			}

			// Rename the temp file to the output file.
			FileSystems.rename(ImmutableList.of(tempFile), ImmutableList.of(outputFile),
					MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
		} catch (IOException e) {
			String msg = e.getMessage();

			LOG.error("Error occurred during decompression of {}", inputFile.toString(), e);
			throw new IOException(sanitizeDecompressionErrorMsg(msg, inputFile, compression));
		}

		return outputFile;
	}

	/**
	 * The error messages coming from the compression library are not consistent
	 * across compression modes. Here we'll attempt to unify the messages to inform
	 * the user more clearly when we've encountered a file which is not compressed
	 * or malformed. Note that GZIP and ZIP compression modes will not throw an
	 * exception when a decompression is attempted on a file which is not
	 * compressed.
	 *
	 * @param errorMsg    The error message thrown during decompression.
	 * @param inputFile   The input file which failed decompression.
	 * @param compression The compression mode used during decompression.
	 * @return The sanitized error message. If the error was not from a malformed
	 *         file, the same error message passed will be returned.
	 */
	private String sanitizeDecompressionErrorMsg(String errorMsg, ResourceId inputFile, Compression compression) {
		if (errorMsg != null
				&& (errorMsg.contains("not in the BZip2 format") || errorMsg.contains("incorrect header check"))) {
			errorMsg = String.format(MALFORMED_ERROR_MSG, inputFile.toString(), compression);
		}

		return errorMsg;
	}

	/**
	 * move file to different directory/bucket
	 * 
	 * @param inputFile
	 */
	private void moveToErrorFolder(ResourceId inputFile) {
		LOG.info(String.format("Starting copy file: %s  to %s: ", inputFile, errorLocation.get()));
		String inputFilename = inputFile.toString();
		String outputFilename = getFileName(inputFilename);

		ResourceId outputDir = FileSystems.matchNewResource(errorLocation.get(), true);
		ResourceId outputFile = outputDir.resolve(outputFilename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);

		try {
			FileSystems.copy(ImmutableList.of(inputFile), ImmutableList.of(outputFile),
					MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
			deleteFile(inputFile);
		} catch (IOException e) {
			String msg = String.format("Error occurred while moving file %s to %s: %s", inputFilename,
					errorLocation.get(), e.getMessage());
			LOG.error(msg, e);
		}
	}

	/**
	 * Remove the files from gcs bucket
	 * 
	 * @param inputFile
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
	 * Get the files name to gcs uri
	 * 
	 * @param inputFile
	 */
	private static String getFileName(String gcsBlobPath) {
		if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
			throw new IllegalArgumentException("GCS blob paths must start with gs://, got " + gcsBlobPath);
		}

		String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
		int firstSlash = bucketAndObjectName.lastIndexOf("/");
		if (firstSlash == -1) {
			throw new IllegalArgumentException(
					"GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
		}
		return bucketAndObjectName.substring(firstSlash + 1);
	}

}
