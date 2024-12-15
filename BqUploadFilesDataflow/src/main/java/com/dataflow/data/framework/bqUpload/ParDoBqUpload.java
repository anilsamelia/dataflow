package com.dataflow.data.framework.bqUpload;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import java.util.List;

/**
 * This class is a user-defined ParDo where its process the elements
 * 
 * @author Anil.Kumar
 * @version 1.0
 * @since 2022-02-08
 */

public class ParDoBqUpload extends PTransform<PCollection<String>, PCollection<String>> {

	private static final long serialVersionUID = 1L;

    private List<JobMetaData> jobMetaDataList;
    private String sourceLocation;
    private String archiveLocation;
    private String exceptionLocation;

    public ParDoBqUpload(List<JobMetaData> jobMetaDataList, String sourceLocation, String archiveLocation, String exceptionLocation) {
        super();
        this.jobMetaDataList = jobMetaDataList;
        this.sourceLocation = sourceLocation;
        this.archiveLocation = archiveLocation;
        this.exceptionLocation = exceptionLocation;
    }

    @Override
    public PCollection<String> expand(PCollection<String> lines) {
        return lines.apply("BQ Upload all the files to table", ParDo.of(new DoFnBqUpload(jobMetaDataList, sourceLocation, archiveLocation, exceptionLocation)));
    }
}
