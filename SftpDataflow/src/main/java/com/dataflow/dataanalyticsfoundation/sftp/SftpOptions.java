package com.dataflow.dataanalyticsfoundation.sftp;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
/**
 * SftpOptions class is user-define class that contains all the input
 * parameters.
 *
 * @author Barkat Dhillon
 * @version 1.0
 * @since 2022-02-08
 */

public interface SftpOptions extends PipelineOptions {

    @Description("Project ID")
    @Default.String("project_id")
    String getProjectId();
    void setProjectId(String value);

    @Description("SFTP Server Host Name")
    String getHost();
    void setHost(String value);

    @Description("SFTP Username secret key name")
    @Validation.Required
    @Default.String("dataflow-username-secret-key")
    String getUsername();
    void setUsername(String value);

    @Description("SFTP Password secret key name")
    @Validation.Required
    @Default.String("dataflow-password-secret-key")
    String getPassword();
    void setPassword(String value);

    @Description("SFTP input folder.")
    @Default.String("landing_folder")
    String getInputFolder();
    void setInputFolder(String value);

    @Description("SFTP input file pattern.")
    @Validation.Required
    @Default.String("*.gz")
    String getInputFilePattern();
    void setInputFilePattern(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    @Default.String("gs://bucket_name/sftp/")
    String getOutputFolder();
    void setOutputFolder(String value);

    @Description("How many maximum connection allowed to SFTP server?")
    String getConcurrency();
    void setConcurrency(String value);

    @Description("Chunk size while streaming file from SFTP to Output folder in GCS bucket")
    String getBufferSize();
    void setBufferSize(String value);

}
