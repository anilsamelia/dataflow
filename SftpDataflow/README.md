# Dataflow job to download files from SFTP Server
This dataflow download files from remote SFTP server, it changes landing directory and then create pCollection of all the files in that folder.
It can scale (parallel download) upto any level as per no. of connection allowed by the remote SFTP server. You can tune the size of chunk to stream from SFTP to GCS bucket, but idealistic size is 1MB, which gave best result. 

## Compilation
mvn clean package -Pdataflow-runner

## Local environment
* Set google credentials: ```export GOOGLE_APPLICATION_CREDENTIALS=<credetials_file.json>```
* Run pipelie locally: ```mvn compile exec:java -Dexec.mainClass=com.mwp.dataanalyticsfoundation.sftp.SftpPipeline```

## Create Dataflow Flex Template
* Set GCR Image Path: ```export TEMPLATE_IMAGE="gcr.io/project_id/data-analytics-foundation/sftp-files:latest"```
* Set Dataflow Template Path: ```export TEMPLATE_PATH="gs://bucket_name/sftp-files"```
* Build Dataflow flex template: ```gcloud dataflow flex-template build $TEMPLATE_PATH --image-gcr-path "$TEMPLATE_IMAGE" --sdk-language "JAVA" --flex-template-base-image JAVA11 --metadata-file "sftp_dataflow_metadata.json" --jar "target/SftpFiles-bundled-1.0-SNAPSHOT.jar" --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.mwp.dataanalyticsfoundation.sftp.SftpPipeline"```
* Deploy Dataflow job from just created flex template: ```gcloud dataflow flex-template run "sftp-files-`date +%Y%m%d-%H%M%S`"     --template-file-gcs-location "$TEMPLATE_PATH"     --service-account-email=mw-data-bq-data-catalog@project_id.iam.gserviceaccount.com --parameters projectId="project_id" --parameters host="sftp.server.com" --parameters username="dataflow-username-secret-key" --parameters password="dataflow-password-secret-key" --parameters inputFolder="/Landing_folder" --parameters inputFilePattern="*.gz" --parameters outputFolder="gs://bucket_name/landing/" --parameters concurrency="5" --parameters bufferSize="8192" --region "us-east1"``` 
