# Dataflow job to bulk upload files
This dataflow pipeline, read a config file which maps which pattern matching files goes to which table and their respective flags like delimiter and skip header or not.

## Compilation
mvn clean package -Pdataflow-runner

## Local environment
* Set google credentials: ```export GOOGLE_APPLICATION_CREDENTIALS=<credetials_file.json>```
* Run pipelie locally: ```mvn compile exec:java -Dexec.mainClass=com.mwp.data.framework.bqUpload.BqUploadPipeline```

## Create Dataflow Flex Template
* Set GCR Image Path: ```export TEMPLATE_IMAGE="gcr.io/project_id/data-analytics-foundation/bq-upload-csv-files:latest"```
* Set Dataflow Template Path: ```export TEMPLATE_PATH="gs://bucket_name/bq-upload-csv-files"```
* Build Dataflow flex template: ```gcloud dataflow flex-template build "$TEMPLATE_PATH" --image-gcr-path "$TEMPLATE_IMAGE" --sdk-language "JAVA" --flex-template-base-image JAVA11 --metadata-file "Bq_upload_GCS_Files_metadata.json" --jar "target/bq-upload-bundled-v1.0.jar" --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.mwp.data.framework.bqUpload.BqUploadPipeline"```
* Deploy Dataflow job from just created flex template: ```gcloud dataflow flex-template run "bq-upload-files"  --template-file-gcs-location "$TEMPLATE_PATH" --service-account-email=mw-data-bq-data-catalog@project_id.iam.gserviceaccount.com --parameters sourceLocation="gs://bucket_name/unzipfiles" --parameters archiveLocation="gs://bucket_name/archive"  --parameters exceptionLocation="gs://bucket_name/exception" --parameters configFileLocation="gs://bucket_name/global-config.csv" --region "us-east1```
