package com.dataflow.dataanalyticsfoundation.sftp;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.jcraft.jsch.ChannelSftp;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
/**
 * SftpPipeline is main class where Dataflow start is execute.
 * 
 * @author Barkat Dhillon
 * @version 1.0
 * @since 2022-02-08
 */

public class SftpPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SftpPipeline.class);

    /**
	 * runDownloadFiles method is used to downloads the files
	 * 
	 * @param SftpOptions   filePath is gcs uri of the files
	*/

    static void runDownloadFiles(SftpOptions options) {
        Pipeline p = Pipeline.create(options);
        String inputFilePattern = options.getInputFilePattern();
        Integer parallelization = -1;
        if (options.getConcurrency() != null) {
            try {
                parallelization = Integer.parseInt(options.getConcurrency());
            } catch (NumberFormatException nfe) {
                LOG.error("Error while parsing concurrency", nfe);
            }
        }
        int bufferSize = 4096;
        if (options.getBufferSize() != null) {
            try {
                bufferSize = Integer.parseInt(options.getBufferSize());
            } catch (NumberFormatException nfe) {
                LOG.error("Error while parsing buffer size", nfe);
            }
        }
        LOG.info("Get username/password form secrets manager");
        String username = null;
        String password = null;

        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName usernameSecret = SecretVersionName.of(options.getProjectId(), options.getUsername(), "latest");
            AccessSecretVersionResponse usernameResponse = client.accessSecretVersion(usernameSecret);
            username = usernameResponse.getPayload().getData().toStringUtf8();

            SecretVersionName passwordSecret = SecretVersionName.of(options.getProjectId(), options.getPassword(), "latest");
            AccessSecretVersionResponse passwordResponse = client.accessSecretVersion(passwordSecret);
            password = passwordResponse.getPayload().getData().toStringUtf8();
        } catch (IOException e) {
            LOG.error("Error while getting secret variables: " + e.getMessage(), e);
        }

        Vector<ChannelSftp.LsEntry> list = null;
        List<String> files = new ArrayList<String>();
        List<String> folders = new ArrayList<String>();
        LOG.info("Get list of files to download...");
        SftpConnection sftpConnection = new SftpConnection(options.getHost(), username, password, options.getInputFolder());
        ChannelSftp channelSftp = sftpConnection.getChannel();
        int idx = inputFilePattern.lastIndexOf(File.separator);
        String folder = null;
        if (idx != -1) {
            folder = inputFilePattern.substring(0, idx);
        }
        try {
            list = channelSftp.ls(inputFilePattern);
            for (ChannelSftp.LsEntry entry : list) {
                if (entry.getAttrs().isDir()) {
                    LOG.info("Folder '"+entry.getFilename()+"' is ignored");
                    folders.add(entry.getFilename());
                } else {
                    String fileName = ((folder == null) ? "" : folder + File.separator) + entry.getFilename();
                    files.add(fileName);
                }
            }
            LOG.info("Total folder(s) ignored in current directory are: "+folders.size());
            LOG.info("Total files to download from sftp server are: " + files.size());
            List<List<String>> subList = divideIntoSublist(files, parallelization);
            LOG.info("Size of pCollection is: " + subList.size());
            p.apply(Create.of(subList))
                    .apply("Download Files", new ParDoDownload(options.getHost(), username, password, options.getInputFolder(), options.getOutputFolder(), bufferSize));

            p.run().waitUntilFinish();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            sftpConnection.disconnect(channelSftp);
        }
    }

    public static void main(String[] args) {
        SftpOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(SftpOptions.class);
        runDownloadFiles(options);
    }

    /**
	 * divideIntoSublist method is used make separately only files list.
	 *
	 * @param List<String> files list
	 * @param int parallelization
	 * return list
	*/
    private static List<List<String>> divideIntoSublist(List<String> list, int parallelization) {
        List<List<String>> sublist = new ArrayList<>();
        int sublistSize = 1;
        if (parallelization != -1) {
            sublistSize = calculateSizeOfQueue(list.size(), parallelization);
        }
        int index = 0;
        int listSize = list.size();
        while (index < listSize) {
            int start = index;
            int end = start + sublistSize;
            if (end > (listSize)) {
                end = listSize;
            }
            sublist.add(list.subList(start, end));
            index = end;
        }

        return sublist;
    }

    private static int calculateSizeOfQueue(int queueSize, int parallelization) {
        return (int) Math.ceil(1.0 * queueSize / parallelization);
    }
}
