package com.mwp.dlp.poc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanDataPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(ScanDataPipeline.class);

    static void runDecryptFiles(ScanDataOptions options) {
        Pipeline p = Pipeline.create(options);
        LOG.info("Read Table");
        p.apply("Read Table", Create.of(options.getTable()))
                .apply("Read BQ Table", MapElements.via(new BiqQueryReader(options.getProjectId(), options.getDatasetId(), options.getMaxLimit())))
                .apply("Inspect DLP", MapElements.via(new DlpInspector(options.getProjectId(), options.getDatasetId(), options.getTable(), options.getInfoTypes())))
                .apply("Publish Tags to Data Catalog", MapElements.via(new DataCatalogPublisher(options.getProjectId(), options.getTagRegion(), options.getDatasetId(), options.getTable(), options.getTagTemplateId(), options.getTagTemplateDisplayName())));

        p.run();
    }


    public static void main(String[] args) {
        ScanDataOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(ScanDataOptions.class);
        runDecryptFiles(options);
    }
}
