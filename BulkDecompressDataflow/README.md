# Dataflow job to download files from SFTP Server
This dataflow work like Google's decompress utility dataflow template, but in dataflow if some error occurred while unzipping it, file moved to error folder and exception is logged, but no error occur, then it will remove the zip file, which will save it from being picked up in the next run.

## Compilation
mvn clean package -Pdataflow-runner

## Local environment
* Set google credentials: ```export GOOGLE_APPLICATION_CREDENTIALS=<credetials_file.json>```
* Run pipelie locally: ```mvn compile exec:java -Dexec.mainClass=com.mwp.dataanalyticsfoundation.decompress.BulkDecompressorPipeline```

## Create Dataflow Flex Template
* Set GCR Image Path: ```export TEMPLATE_IMAGE="gcr.io/project_id/data-analytics-foundation/bulk-decompress:latest"```
* Set Dataflow Template Path: ```export TEMPLATE_PATH="gs://bucket_name/bulk-decompress"```
* Build Dataflow flex template: ```gcloud dataflow flex-template build $TEMPLATE_PATH --image-gcr-path "$TEMPLATE_IMAGE" --sdk-language "JAVA" --flex-template-base-image JAVA11 --metadata-file "bulk_decompress_dataflow_metadata.json" --jar "target/BulkDecompress-bundled-1.0-SNAPSHOT.jar" --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.mwp.dataanalyticsfoundation.decompress.BulkDecompressorPipeline"```
* Deploy Dataflow job from just created flex template: ```gcloud dataflow flex-template run "bulk-decompress-`date +%Y%m%d-%H%M%S`"     --template-file-gcs-location "$TEMPLATE_PATH"     --service-account-email=mw-data-bq-data-catalog@project_id.iam.gserviceaccount.com --parameters inputFilePattern="gs://bucket_name/landing/*.gz" --parameters outputDirectory="gs://bucket_name/decompressed/" --parameters errorDirectory="gs://bucket_name/error/" --region "us-east1"``` 
