RUN ON LOCAL

```python run.py --config ..\local-dev\files\techshed\data-pump-services\data-pump-mirror-sync-bq\config.yml```

Build Docker:

```docker build -t bq_stream:1 .```

RUN
```docker run -it bq_stream:1```

BUILD IN GCR CONTAINER REGISTRY

 ```docker build -t gcr.io/{project_id}/bq/bq_stream:latest .```
 ```docker push gcr.io/{project_id}/bq/bq_stream:latest ```

 Purpose of this job

 ``` The main task of this job is to create/replace views and encrypted columns for the schemas: MIRROR, MIRROR_LOG, CONF, and CONF_LOG. ```
 ``` This job replaces the views if any new column is altered in Postgres or if a new encrypted column is added/Removed. ```


