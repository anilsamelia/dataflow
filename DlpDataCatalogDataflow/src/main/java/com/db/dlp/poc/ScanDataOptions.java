package com.mwp.dlp.poc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ScanDataOptions extends PipelineOptions {

    @Description("Project ID")
    @Default.String("project_id")
    String getProjectId();
    void setProjectId(String value);

    @Description("Big Query Dataset")
    @Validation.Required
    @Default.String("barkat_dataset")
    String getDatasetId();
    void setDatasetId(String value);
    
    @Description("Big Query Table Name")
    @Validation.Required
    @Default.String("barkat_table")
    String getTable();
    void setTable(String value);

    @Description("Max Limit to query Big Query Data")
    @Validation.Required
    @Default.Long(1000)
    Long getMaxLimit();
    void setMaxLimit(Long value);

    @Description("DLP Inspection Info Types.")
    @Validation.Required
    @Default.String("PHONE_NUMBER/EMAIL_ADDRESS/CREDIT_CARD_NUMBER")
    String getInfoTypes();
    void setInfoTypes(String value);

    @Description("Data Catalog Tag Template Region")
    @Validation.Required
    @Default.String("us-east1")
    String getTagRegion();
    void setTagRegion(String value);

    @Description("Data Catalog Tag Template ID")
    @Validation.Required
    @Default.String("dlp_data_catalog_template")
    String getTagTemplateId();
    void setTagTemplateId(String value);

    @Description("Data Catalog Tag Display Name")
    @Validation.Required
    @Default.String("DLP Data Catalog Template")
    String getTagTemplateDisplayName();
    void setTagTemplateDisplayName(String value);
}
