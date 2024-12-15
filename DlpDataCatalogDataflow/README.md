#Dataflow job to scan Big Query data and publish tags to Data Catalog  
This dataflow demonstrate how DLP API can be used to scan BiQuery table and publish DataCatalog tags and apply policy tags on BigQuery table.

## Compilation
mvn clean package -Pdataflow-runner

## Local environment
* Set google credentials: ```export GOOGLE_APPLICATION_CREDENTIALS=<credetials_file.json>```
* Run pipeline locally: ```mvn compile exec:java -Dexec.mainClass=com.mwn.dlp.poc.ScanDataPipeline```

## Create Dataflow Flex Template
* Set GCR Image Path: ```export TEMPLATE_IMAGE="gcr.io/project_id/dlp/bq-dlp-scan:latest"```
* Set Dataflow Template Path: ```export TEMPLATE_PATH="gs://bucket_name/bq-dlp-scan"```
* Build Dataflow flex template: ```gcloud dataflow flex-template build $TEMPLATE_PATH --image-gcr-path "$TEMPLATE_IMAGE" --sdk-language "JAVA" --flex-template-base-image JAVA11 --metadata-file "bq_dlp_scan_metadata.json" --jar "target/ScanData-bundled-1.0-SNAPSHOT.jar" --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.mwp.dlp.poc.ScanDataPipeline"```
* Deploy Dataflow job from just created flex template: ```gcloud dataflow flex-template run "bq-dlp-scan-`date +%Y%m%d-%H%M%S`"     --template-file-gcs-location "$TEMPLATE_PATH"  --region "us-east1" --parameters projectId="project_id" --parameters tagRegion="us-east1" --parameters datasetId="frank_work" --parameters table="barkat_table"  --parameters maxLimit="1000" --parameters tagTemplateId="dlp_data_catalog_template" --parameters tagTemplateDisplayName="DLP Data Catalog Template" --parameters infoTypes="PHONE_NUMBER/EMAIL_ADDRESS/CREDIT_CARD_NUMBER"``` 
