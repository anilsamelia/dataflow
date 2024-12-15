package com.dataflow.dataanalyticsfoundation.sftp;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
/**
 * This class is a user-defined DoFn where its process the elements
 * 
 * @author Barkat Dhillon
 * @version 1.0
 * @since 2022-02-08
 */

public class DoFnDowload extends DoFn<List<String>, List<String>> {
    private static final int KB = 1024;
    private static final int MB = KB * KB;
    private static final Logger LOG = LoggerFactory.getLogger(DoFnDowload.class);
    private static final String GCS_PATH_PREFIX = "gs://";

    private String host;
    private String username;
    private String password;
    private String inputFolder;
    private String outputFolder;
    private int bufferSize = MB;

    public DoFnDowload(String host, String username, String password, String inputFolder, String outputFolder, int bufferSize) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.inputFolder = inputFolder;
        this.outputFolder = outputFolder;
        this.bufferSize = bufferSize;
    }

    @ProcessElement
    public void processElement(@Element List<String> files, OutputReceiver<List<String>> receiver) throws IOException, SftpException {
        Storage storage = StorageOptions.newBuilder().build().getService();
        SftpConnection sftpConnection = new SftpConnection(host, username, password, inputFolder);
        ChannelSftp channelSftp = sftpConnection.getChannel();

        for (String filePath : files) {
            downloadFile(channelSftp, storage, filePath);
        }
        sftpConnection.disconnect(channelSftp);
        receiver.output(files);
    }

	/**
	 * downloadFile method is used to download the file from GCS
	 * 
	 * @param ChannelSftp channelSftp
	 * @param Storage storage
	 * @param String  filePath
	 * 
	 */

    
    private void downloadFile(ChannelSftp channelSftp, Storage storage, String filePath) throws IOException, SftpException {
        LOG.info("Streaming SFTP file: \"" + filePath + "\" to: \"" + outputFolder + filePath + "\"");
        byte[] buffer = new byte[bufferSize];
        BufferedInputStream bis = new BufferedInputStream(channelSftp.get(filePath));

        String bucketName = getBucketName(outputFolder);
        String folderPath = getObjectName(outputFolder);

        BlobId blobId = BlobId.of(bucketName, folderPath + filePath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        WriteChannel writer = storage.writer(blobInfo);

        int readCount;
        while ((readCount = bis.read(buffer)) > 0) {
            writer.write(ByteBuffer.wrap(buffer));
        }
        writer.close();
        LOG.info("Downloaded the file: \"" + filePath + "\" successfully!!");
        bis.close();
    }

    private static String getBucketName(String gcsBlobPath) {
        if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
            throw new IllegalArgumentException(
                    "GCS blob paths must start with gs://, got " + gcsBlobPath);
        }

        String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
        int firstSlash = bucketAndObjectName.indexOf("/");
        if (firstSlash == -1) {
            throw new IllegalArgumentException(
                    "GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
        }
        return bucketAndObjectName.substring(0, firstSlash);
    }

    private static String getObjectName(String gcsBlobPath) {
        if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
            throw new IllegalArgumentException(
                    "GCS blob paths must start with gs://, got " + gcsBlobPath);
        }

        String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
        int firstSlash = bucketAndObjectName.indexOf("/");
        if (firstSlash == -1) {
            throw new IllegalArgumentException(
                    "GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
        }
        return bucketAndObjectName.substring(firstSlash + 1);
    }

}
