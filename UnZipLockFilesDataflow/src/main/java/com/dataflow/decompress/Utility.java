package com.dataflow.decompress;

/**
 * BqUploadUtility class is a user-defined class that contains re-usable
 * methods. parameters.
 *
 * @author Anil.Kumar
 * @version 1.0
 * @since 2022-02-08
 */
public class Utility {

	public static final String GCS_PATH_PREFIX = "gs://";

	/**
	 * getBucketName method is read the bucket Name from gcs URI path
	 * 
	 * @param String gcsBlobPath GCS file uri
	 * @return String bucket name
	 */
	public static String getBucketName(String gcsBlobPath) {
		if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
			throw new IllegalArgumentException("GCS blob paths must start with gs://, got " + gcsBlobPath);
		}

		String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
		int firstSlash = bucketAndObjectName.indexOf("/");
		if (firstSlash == -1) {
			throw new IllegalArgumentException(
					"GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
		}
		return bucketAndObjectName.substring(0, firstSlash);
	}

	/**
	 * getObjectName method is read the bucket Name from gcs URI path
	 * 
	 * @param String gcsBlobPath GCS file uri
	 * @return String Object name
	 */
	public static String getObjectName(String gcsBlobPath) {
		if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
			throw new IllegalArgumentException("GCS blob paths must start with gs://, got " + gcsBlobPath);
		}

		String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
		int firstSlash = bucketAndObjectName.indexOf("/");
		if (firstSlash == -1) {
			throw new IllegalArgumentException(
					"GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
		}
		return bucketAndObjectName.substring(firstSlash + 1);
	}

	/**
	 * getFileName method is read the file Name from gcs URI path
	 * 
	 * @param String gcsBlobPath GCS file uri
	 * @return String file name
	 */
	public static String getFileName(String gcsBlobPath) {
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

	public static String suffixDir(String inputFilePattren) {
		String s[] = inputFilePattren.split("/");
		String dir = "";
		for (int i = 0; i < s.length - 1; i++) {
			dir += s[i] + "/";
		}
		return dir;
	}

}
