package com.mwp.dlp.poc;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.privacy.dlp.v2.*;
import com.google.type.Date;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.stream.Collectors;

public class DlpInspector extends SimpleFunction<KV<String, List<String>>, Hashtable<String, InspectResult>> {

    private static final Logger LOG = LoggerFactory.getLogger(DlpInspector.class);
    private static boolean VERBOSE_OUTPUT = true; // toggle for more detailed output
    private static int MAX_REQUEST_BYTES = 480000; // max request size in bytes
    private static int MAX_REQUEST_CELLS = 50000; // max cell count per request
    private String projectId;
    private String datasetId;
    private String tableName;
    private String infoTypeString;

    public DlpInspector(String  projectId, String datasetId, String  tableName, String infoTypeString){
        this.projectId = projectId;
        this.datasetId= datasetId;
        this.tableName = tableName;
        this.infoTypeString = infoTypeString;
    }

    @Override
    public Hashtable<String, InspectResult> apply(KV<String, List<String>> input) {
        LOG.info("Inspect DLP");
        System.out.println(input.getKey());
        System.out.println(input.getValue().size());
        DlpServiceClient dlpServiceClient = null;
        try {
            dlpServiceClient = DlpServiceClient.create();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        int sentCount = 0;
        int prevSentCount = 0;
        java.util.Hashtable<String, InspectResult> splitMap =
                new java.util.Hashtable<String, InspectResult>();
        int splitTotal = 0;
        List<Table.Row> rows = new ArrayList<>();
        String headerNames = input.getKey();
        List<FieldId> headers =
                Arrays.stream(headerNames.split(","))
                        .map(header -> FieldId.newBuilder().setName(header).build())
                        .collect(Collectors.toList());
        List<String> records =  input.getValue();
        for (String r:records) {
            rows.add(convertCsvRowToTableRow(r));
        }

        while (sentCount < rows.size()) {
            splitTotal++;
            try {
                List<Table.Row> subRows = getMaxRows(rows, sentCount, headers.size());
                prevSentCount = sentCount;
                sentCount = sentCount + subRows.size();
                splitMap.put(
                        "result" + sentCount,
                        inspectRows(getProjectName(projectId), dlpServiceClient, headers, subRows));
                if (VERBOSE_OUTPUT) {
                    System.out.println("|");
                    System.out.println(
                            "[ Request Size : request#"
                                    + splitTotal
                                    + " | start-row="
                                    + sentCount
                                    + " | row-count="
                                    + subRows.size()
                                    + " | cell-count="
                                    + subRows.size() * headers.size()
                                    + "]");
                }
            } catch (Exception err) {
                if (err.getMessage().contains("exceeds limit")
                        || err.getMessage().contains("only 50,000 table values are allowed")) {
                    // This message should not happen since we are measuring size before
                    // sending.  So if it happens, it is a hard fail as something is
                    // wrong.
                    System.out.println("|");
                    System.out.print("*** Fatal Error ***");
                    throw err;
                } else if (err.getMessage().contains("DEADLINE_EXCEEDED")) {
                    // reset sentCount to prev so will make it retry;
                    System.out.print("*** deadline_exceeded / retry ***");
                    sentCount = prevSentCount;
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("|");
                    System.out.println(
                            "*** Unknown Fatal Error when trying to inspect data ***");
                    throw err;
                }
            }
        }

        return splitMap;
    }

    private Table.Row convertCsvRowToTableRow(String row) {
        // Complex split that allows quoted commas
        String[] values = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        Table.Row.Builder tableRowBuilder = Table.Row.newBuilder();
        int i = 0;
        for (String value : values) {
            i++;
            LocalDate date = getValidDate(value);
            if (date != null) {
                // convert to com.google.type.Date
                Date dateValue =
                        Date.newBuilder()
                                .setYear(date.getYear())
                                .setMonth(date.getMonthValue())
                                .setDay(date.getDayOfMonth())
                                .build();
                Value tableValue = Value.newBuilder().setDateValue(dateValue).build();
                tableRowBuilder.addValues(tableValue);
            } else {
                tableRowBuilder.addValues(Value.newBuilder().setStringValue(value).build());
            }
        }
        return tableRowBuilder.build();
    }

    // Parse string to valid date, return null when invalid
    private LocalDate getValidDate(String dateString) {
        try {
            return LocalDate.parse(dateString);
        } catch (DateTimeParseException e) {
            return null;
        }
    }


    // this method returns rows that are under the max bytes and cell count for a DLP request.
    private List getMaxRows(List rows, int startRow, int headerCount) throws Exception {
        ArrayList<Table.Row> subRows = null;

        // estimate the request size
        int estimatedMaxRowsBytes =
                MAX_REQUEST_BYTES
                        / (getBytesFromList(rows) / rows.size()); // average could be off if rows differ a lot
        int estimatedMaxRowsCells =
                MAX_REQUEST_CELLS
                        / headerCount; // pretty close to the max since every rows has the same count

        // we want the smallest of the two
        int estimatedMaxRows = estimatedMaxRowsBytes;
        if (estimatedMaxRowsCells < estimatedMaxRowsBytes) {
            estimatedMaxRows = estimatedMaxRowsCells;
        }
        int estimatedEndRows = startRow + estimatedMaxRows;
        if (estimatedEndRows > rows.size()) {
            estimatedEndRows = rows.size();
        }
        subRows = new ArrayList<Table.Row>(rows.subList(startRow, estimatedEndRows));

        // in case something is too bill this will remove one row at a time until it's under the limits.
        while (getBytesFromList(subRows) > MAX_REQUEST_BYTES
                || (subRows.size() * headerCount) > MAX_REQUEST_CELLS) {
            if (subRows.size() > 0) {
                subRows.remove(subRows.size() - 1);
            } else {
                throw new Exception("Single Row greater than max size - not currently supported");
            }
        }
        return subRows;
    }

    // this methods calcualtes the total bytes of a list of rows.
    private int getBytesFromList(List list) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(baos);
        out.writeObject(list);
        out.close();
        return baos.toByteArray().length;
    }

    // Takes rows/header and generates inspection results from DLP content.inspect
    private InspectResult inspectRows(
            String parentName, DlpServiceClient dlpServiceClient, List<FieldId> headers, List<Table.Row> rows) {
        // Inspect the table for info types

        Table table = Table.newBuilder().addAllHeaders(headers).addAllRows(rows).build();
        List<InfoType> infoTypes =
                Arrays.stream(infoTypeString.split("/"))
                        .map(it -> InfoType.newBuilder().setName(it).build())
                        .collect(Collectors.toList());

        // Specify how the content should be inspected.
        InspectConfig inspectConfig =
                InspectConfig.newBuilder().addAllInfoTypes(infoTypes).setIncludeQuote(true).build();

        ContentItem tableItem = ContentItem.newBuilder().setTable(table).build();
        InspectContentRequest request =
                InspectContentRequest.newBuilder()
                        .setParent(parentName)
                        .setInspectConfig(inspectConfig)
                        .setItem(tableItem)
                        .build();
        try {
            InspectContentResponse response = dlpServiceClient.inspectContent(request);
            return response.getResult();
        } catch (Exception e2) {
            e2.printStackTrace();
            throw e2;
        }
    }

    private String getProjectName(String inS) {
        return "projects/" + inS;
    }
}
