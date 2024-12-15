package com.mwp.dlp.poc;

import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BiqQueryReader extends SimpleFunction<String, KV<String, List<String>>> {
    private static final Logger LOG = LoggerFactory.getLogger(BiqQueryReader.class);
    String projectId;
    String datasetId;
    Long maxLimit;

    public BiqQueryReader(String projectId, String datasetId, Long maxLimit) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.maxLimit = maxLimit;
    }

    @Override
    public KV<String, List<String>>  apply(String tableName) {
        LOG.info("Read BQ Table");
        KV<String, List<String>> output  = null;
        BigQueryOptions options = BigQueryOptions.newBuilder().setProjectId(projectId).build();
        BigQuery bigQuery = options.getService();

        String query = String.format("select * from %s.%s limit %d", datasetId, tableName, maxLimit);
        System.out.println(query);
        QueryJobConfiguration queryJobConfig = QueryJobConfiguration.newBuilder(query).setUseQueryCache(true).build();
        try {
            TableResult result = bigQuery.query(queryJobConfig);
            String headerNames = "";
            FieldList columns = result.getSchema().getFields();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 1) {
                    headerNames = headerNames + "," + columns.get(i).getName();
                } else {
                    headerNames = columns.get(i).getName();
                }
            }
            List<String> list = createCommaSeparatedRows(result);
            output = KV.of(headerNames, list);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return output;
    }

    private List<String> createCommaSeparatedRows(TableResult result){
        List<String> rows = new ArrayList<String>();
        if (result != null) {
            for (FieldValueList row : result.iterateAll()) {
                Iterator<FieldValue> fields = row.iterator();
                int i=0;
                String record = "";
                while(fields.hasNext()){
                    String theValue = fields.next().getStringValue();
                    if (theValue == null) {
                        theValue = "";
                    }
                    if (i > 1) {
                        record = record + "," + theValue.replace(",", "-");
                    } else {
                        record = theValue.replace(",", "-");
                    }
                    i++;
                }
                rows.add(record);
            }
        }
        return rows;
    }
}
