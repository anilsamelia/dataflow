package com.dataflow.dataanalyticsfoundation.sftp;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
/**
 * This class is a user-defined ParDo where its process the elements
 * 
 * @author Barkat Dhillon
 * @version 1.0
 * @since 2022-02-08
 */

public class ParDoDownload extends PTransform<PCollection<List<String>>, PCollection<List<String>>> {

    private static final Logger LOG = LoggerFactory.getLogger(ParDoDownload.class);

    private String host;
    private String username;
    private String password;
    private String inputFolder;
    private String outputFolder;
    private int bufferSize;

    public ParDoDownload(String host, String username, String password, String inputFolder, String outputFolder, int bufferSize) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.inputFolder = inputFolder;
        this.outputFolder = outputFolder;
        this.bufferSize = bufferSize;
    }

    @Override
    public PCollection<List<String>> expand(PCollection<List<String>> lines) {
        return lines.apply("Download the file", ParDo.of(new DoFnDowload(host, username, password, inputFolder, outputFolder, bufferSize)));
    }
}
