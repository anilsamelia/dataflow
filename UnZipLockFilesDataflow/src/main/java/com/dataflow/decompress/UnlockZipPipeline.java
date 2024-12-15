package com.dataflow.decompress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.text.DecimalFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import net.lingala.zip4j.io.inputstream.ZipInputStream;
import net.lingala.zip4j.model.LocalFileHeader;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;

public class UnlockZipPipeline {

	private static final Logger LOG = LoggerFactory.getLogger(UnlockZipPipeline.class);

	public static class UnlockZip extends PTransform<PCollection<String>, PCollection<String>> {
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		static final String LOCAL_DIR = "temp_output";
		String sourceDirectory;
		String bucketName;
		String outputDirectory;
		String password;
		String outputFailureFile;
		String streamBufferSize;
		String exceptionLocation;
		String inputFilePattern;

		public UnlockZip(String sourceDirectory, String outputDirectory, String password, String streamBufferSize,
				String exceptionLocation, String inputFilePattern) {
			this.sourceDirectory = sourceDirectory;
			this.outputDirectory = outputDirectory;
			this.password = password;
			this.streamBufferSize = streamBufferSize;
			this.exceptionLocation = exceptionLocation;
			this.inputFilePattern = inputFilePattern;
		}

		@Override
		public PCollection<String> expand(PCollection<String> input) {
			PCollection<String> words = input.apply("Unzip the file", ParDo.of(new UnlockZipFn(sourceDirectory,
					outputDirectory, password, streamBufferSize, exceptionLocation, inputFilePattern)));
			return words;
		}
	}

	public static class UnlockZipFn extends DoFn<String, String> {

		private static final long serialVersionUID = 1L;

		private static final String GCS_PATH_PREFIX = "gs://";
		String sourceDirectory;
		String bucketName;
		String outputDirectory;
		String password;
		String outputFailureFile;
		Integer streamBufferSize;
		String exceptionLocation;
		String inputFilePattern;

		public UnlockZipFn(String sourceDirectory, String outputDirectory, String password, String streamBufferSize,
				String exceptionLocation, String inputFilePattern) {
			this.sourceDirectory = sourceDirectory;
			this.outputDirectory = outputDirectory;
			this.password = password;
			this.streamBufferSize = Integer.parseInt(streamBufferSize);
			this.exceptionLocation = exceptionLocation;
			this.inputFilePattern = inputFilePattern;

		}

		@ProcessElement
		public void processElement(@Element String element, OutputReceiver<String> receiver) throws IOException {
			LOG.info("Starting unlock of '" + element + "' file");
			String gcsBlobPath = element;
			String srcBucketName = Utility.getBucketName(gcsBlobPath);
			String objectName = Utility.getObjectName(gcsBlobPath);
			String fileName = Utility.getFileName(gcsBlobPath);
			String destBucketName = Utility.getBucketName(outputDirectory);
			String destFolder = Utility.getObjectName(outputDirectory);
			String localZipFilePath = UnlockZip.LOCAL_DIR + FileSystems.getDefault().getSeparator() + fileName;
			Storage storage = StorageOptions.newBuilder().build().getService();
			BlobId blobId = BlobId.of(srcBucketName, objectName);
			Blob blob = storage.get(blobId);
			if (blob.getContentType().equals("application/zip")) {
				File folder = new File(UnlockZip.LOCAL_DIR);
				folder.mkdir();
				File tempFileName = new File(localZipFilePath);
				LOG.info("Downloading '" + element + "' file...");
				downloadFile(storage, tempFileName, srcBucketName, objectName);
				try {
					LOG.info("Going to unlock zip file: " + objectName + " of size: " + getFileSize(tempFileName));
					extractAndUploadToGcs(tempFileName, password.toCharArray(), storage, destBucketName, destFolder);
					LOG.info("Unlocked zip file: " + objectName);
				} catch (Exception e) {
					LOG.error("Error:" + e);
					moveObject(element, exceptionLocation, storage);
				} finally {
					tempFileName.delete();
					folder.delete();
				}
				if (storage.delete(srcBucketName, objectName)) {
					LOG.info(objectName + " is deleted from " + srcBucketName);
				}
			} else {
				receiver.output("Fail to unzip");
				throw new RuntimeException("File format is incorrect");
			}
			receiver.output("uploaded successfully");
		}



		private void moveObject(String sourceFileUri, String toTarget, Storage storage) {
			String sourceBucket = Utility.getBucketName(sourceFileUri);
			String targetBucketName = Utility.getBucketName(toTarget);
			String fileName = Utility.getFileName(sourceFileUri);
			String object = Utility.getObjectName(sourceFileUri);
			LOG.error(String.format("Start move to the file: " + toTarget));
			Blob blob = storage.get(sourceBucket, object);
			String[] fileSplit = fileName.split("/");
			String archiveFileName = fileSplit[fileSplit.length - 1];
			String movedFileName = archiveFileName.split(".zip")[0] + ".zip";
			String newObject = getDirectoryPrefix(toTarget) + Utility.suffixDir(inputFilePattern) + movedFileName;
			if (blob.copyTo(targetBucketName, newObject).isDone()) {
				LOG.info("File is created : gs://" + targetBucketName + "/" + Utility.suffixDir(inputFilePattern) + newObject);
				if (blob.delete()) {
					LOG.info(String.format("Object is delete from %s",
							"gs://" + blob.getBucket() + "/" + blob.getName()));
				} else {
					String logerr = String.format("Error Fail to delete from %s",
							"gs://" + blob.getBucket() + "/" + blob.getName());
					LOG.error(logerr);
					throw new RuntimeException(logerr);
				}
			} else {
				LOG.error("Fail to move the object gs://" + blob.getBucket() + "/" + blob.getName());
			}
		}

		private static String getDirectoryPrefix(String gcsBlobPath) {
			return gcsBlobPath.substring(6 + Utility.getBucketName(gcsBlobPath).length(), gcsBlobPath.length());
		}

		private void downloadFile(Storage storage, File tempfile, String bucketName, String objectName)
				throws IOException {
			LOG.info("Start download " + objectName + " file ");
			OutputStream os = new FileOutputStream(tempfile);
			WritableByteChannel outChannel = Channels.newChannel(os);
			try (ReadChannel reader = storage.reader(bucketName, objectName)) {
				ByteBuffer bytes = ByteBuffer.allocate(streamBufferSize);
				while (reader.read(bytes) > 0) {
					bytes.flip();
					outChannel.write(bytes);
					bytes.clear();
				}
			}
			outChannel.close();
			os.close();
			LOG.info(objectName + " is downloaded successfully ");
		}

		private void extractAndUploadToGcs(File tempFile, char[] password, Storage storage, String destBucketName,
				String destFolder) throws IOException {
			String fileName = new ZipInputStream(new FileInputStream(tempFile), password).getNextEntry().getFileName();
			BlobInfo blobInfo = BlobInfo
					.newBuilder(BlobId.of(destBucketName, destFolder + Utility.suffixDir(inputFilePattern) + fileName)).build();
			LOG.info("Started extracting to " + blobInfo.getBlobId().getName());
			WriteChannel writer = storage.writer(blobInfo);
			LocalFileHeader localFileHeader;
			int readLen;
			byte[] readBuffer = new byte[streamBufferSize];
			InputStream inputStream = new FileInputStream(tempFile);
			try (ZipInputStream zipInputStream = new ZipInputStream(inputStream, password)) {
				while ((localFileHeader = zipInputStream.getNextEntry()) != null) {
					while ((readLen = zipInputStream.read(readBuffer)) != -1) {
						writer.write(ByteBuffer.wrap(readBuffer, 0, readLen));
					}
				}
			}
			inputStream.close();
			writer.close();
			LOG.info("Extraction successfully to " + blobInfo.getBlobId().getName());
		}



	}

	static void runUnlockFiles(UnlockZipOptions options) throws PipelineExecutionException {
		Pipeline p = Pipeline.create(options);
		String inputPattern = options.getSourceDirectory() + options.getInputFilePattern();
		String password = getSecretsManagerPassword(options);
		p.apply("Read Input File Pattern", FileIO.match().filepattern(inputPattern))
				.apply("Reading matching files", FileIO.readMatches()).apply("Create PCollection",
						MapElements.into(TypeDescriptors.strings()).via((FileIO.ReadableFile file) -> {
							String fileName = file.getMetadata().resourceId().toString();
							LOG.debug("Fanning out files -> " + fileName);
							return fileName;
						}))
				.apply("ParDo on each file in the PCollection",
						new UnlockZip(options.getSourceDirectory(), options.getOutputDirectory(), password,
								options.getStreamBufferSize(), options.getExceptionLocation(),
								options.getInputFilePattern()));
		p.run();
	}

	static String getSecretsManagerPassword(UnlockZipOptions options) {
		LOG.info("Get password form secrets manager");
		String password = null;
		try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
			String projectId = StorageOptions.getDefaultInstance().getProjectId();
			SecretVersionName secretVersionName = SecretVersionName.of(projectId, options.getSecretPasswordVariable(),
					"latest");
			AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
			password = response.getPayload().getData().toStringUtf8();
		} catch (IOException e) {
			LOG.error("Check scrert Variable " + e.getMessage());
		}
		LOG.info("Password got from secrets manager");
		return password;
	}

	private static String getFileSize(File file) throws IOException {
		long size = Files.size(file.toPath());
		DecimalFormat df = new DecimalFormat("0.00");

		float sizeKb = 1024.0f;
		float sizeMb = sizeKb * sizeKb;
		float sizeGb = sizeMb * sizeKb;
		float sizeTerra = sizeGb * sizeKb;

		if (size < sizeMb)
			return df.format(size / sizeKb) + " Kb";
		else if (size < sizeGb)
			return df.format(size / sizeMb) + " Mb";
		else if (size < sizeTerra)
			return df.format(size / sizeGb) + " Gb";

		return "";
	}

	public static void main(String[] args) {
		UnlockZipOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(UnlockZipOptions.class);
		runUnlockFiles(options);
	}
}
