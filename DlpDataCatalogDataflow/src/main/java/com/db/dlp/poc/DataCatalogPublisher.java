package com.mwp.dlp.poc;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.services.datacatalog.v1beta1.DataCatalog;
import com.google.api.services.datacatalog.v1beta1.model.GoogleCloudDatacatalogV1beta1Tag;
import com.google.api.services.datacatalog.v1beta1.model.GoogleCloudDatacatalogV1beta1TagField;
import com.google.cloud.datacatalog.v1beta1.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.privacy.dlp.v2.Finding;
import com.google.privacy.dlp.v2.InspectResult;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class DataCatalogPublisher extends SimpleFunction<Hashtable<String, InspectResult>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(DataCatalogPublisher.class);
    private static boolean VERBOSE_OUTPUT = true; // toggle for more detailed output
    private static Cache<String, Object> dataCatalogLocalCache =
            CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(600, TimeUnit.SECONDS).build();


    String projectId;
    String region;
    String datasetId;
    String tableName;
    String tagTemplateID;
    String tagTemplateDisplayName;


    public DataCatalogPublisher(String projectId, String region, String datasetId, String tableName, String tagTemplateID, String tagTemplateDisplayName){
        this.projectId = projectId;
        this.region = region;
        this.datasetId = datasetId;
        this.tableName = tableName;
        this.tagTemplateID = tagTemplateID;
        this.tagTemplateDisplayName = tagTemplateDisplayName;
    }

    @Override
    public String apply(Hashtable<String, InspectResult> input) {
        LOG.info("Publish Tags to Data Catalog");
        DataCatalogClient dataCatalogClient = null;
        try {
            dataCatalogClient = DataCatalogClient.create();
        } catch (IOException e) {
            e.printStackTrace();
        }
        DataCatalog dataCatalogBatchClient = DataCatalogBatchClient.getBatchClient();

        java.util.Hashtable<String, java.util.Hashtable> columnHash =
                new java.util.Hashtable<>();
        for (InspectResult result : input.values()) {
            if (result != null && result.getFindingsCount() > 0) {
                for (Finding finding : result.getFindingsList()) {
                    String column =
                            finding
                                    .getLocation()
                                    .getContentLocations(0)
                                    .getRecordLocation()
                                    .getFieldId()
                                    .getName();
                    String infoType = finding.getInfoType().getName();
                    java.util.Hashtable<String, Integer> tempCol = columnHash.get(column);
                    if (tempCol == null) {
                        tempCol = new java.util.Hashtable<String, Integer>();
                    }
                    Integer tempCount = tempCol.get(infoType);
                    if (tempCount == null) {
                        tempCount = 0;
                    }
                    tempCol.put(infoType, tempCount + 1);
                    columnHash.put(column, tempCol);
                }
            }
        }

        BatchRequest batchRequest = DataCatalogBatchClient.getBatchRequest();
        for (String columnName : columnHash.keySet()) {
            if (VERBOSE_OUTPUT) {
                System.out.println(">> Column [" + columnName + "] has findings");
            } else {
                System.out.print(".");
            }
            java.util.Hashtable<String, Integer> tempCol = columnHash.get(columnName);

            if (getTopInfoTypeMin(tempCol) >= 1) {

                try {
                    String topInfoType = getTopInfoType(tempCol);

                    TagTemplate tagTemplate = null;

                    String tagTemplateName =
                            String.format("projects/%s/locations/%s/tagTemplates/%s", projectId, region,tagTemplateID);
                    // try to load the template
                    try {

                        Object cachedTagTemplate = dataCatalogLocalCache.getIfPresent(tagTemplateName);

                        if (cachedTagTemplate != null) {
                            tagTemplate = (TagTemplate) cachedTagTemplate;
                            if (VERBOSE_OUTPUT) {
                                System.out.println("[CacheHit] - getTagTemplate");
                            }
                        } else {
                            tagTemplate = dataCatalogClient.getTagTemplate(tagTemplateName);
                            dataCatalogLocalCache.put(tagTemplateName, tagTemplate);
                            if (VERBOSE_OUTPUT) {
                                System.out.println("[CacheMiss] - getTagTemplate");
                            }
                        }
                    } catch (Exception e) {
                        if (VERBOSE_OUTPUT) {
                            System.out.println("Template not found, creating");
                        } else {
                            System.out.print(".");
                        }
                    }
                    if (tagTemplate == null) {
                        // Failed to load so Create the Tag Template.
                        TagTemplateField topInfoTypeField =
                                TagTemplateField.newBuilder()
                                        .setDisplayName("top_info_type")
                                        .setType(FieldType.newBuilder().setPrimitiveType(FieldType.PrimitiveType.STRING).build())
                                        .build();

                        TagTemplateField infoTypesDetailsField =
                                TagTemplateField.newBuilder()
                                        .setDisplayName("info_types_detail")
                                        .setType(FieldType.newBuilder().setPrimitiveType(FieldType.PrimitiveType.STRING).build())
                                        .build();
                        TagTemplateField runIdField =
                                TagTemplateField.newBuilder()
                                        .setDisplayName("run_id")
                                        .setType(FieldType.newBuilder().setPrimitiveType(FieldType.PrimitiveType.STRING).build())
                                        .build();

                        TagTemplateField lastUpdatedField =
                                TagTemplateField.newBuilder()
                                        .setDisplayName("last_updated")
                                        .setType(FieldType.newBuilder().setPrimitiveType(FieldType.PrimitiveType.STRING).build())
                                        .build();

                        tagTemplate =
                                TagTemplate.newBuilder()
                                        .setDisplayName(tagTemplateDisplayName)
                                        .putFields("top_info_type", topInfoTypeField)
                                        .putFields("info_types_detail", infoTypesDetailsField)
                                        .putFields("last_updated", lastUpdatedField)
                                        .putFields("run_id", runIdField)
                                        .build();

                        CreateTagTemplateRequest createTagTemplateRequest =
                                CreateTagTemplateRequest.newBuilder()
                                        .setParent(
                                                LocationName.newBuilder()
                                                        .setProject(projectId)
                                                        .setLocation(region)
                                                        .build()
                                                        .toString())
                                        .setTagTemplateId(tagTemplateID)
                                        .setTagTemplate(tagTemplate)
                                        .build();

                        // when running with many threads, it's possible that a new template gets created at the
                        // same time. So we want to catch this.
                        try {
                            tagTemplate = dataCatalogClient.createTagTemplate(createTagTemplateRequest);
                            if (VERBOSE_OUTPUT) {
                                System.out.println(String.format("Created template: %s", tagTemplate.getName()));
                            } else {
                                System.out.print("+");
                            }
                        } catch (Exception e) {
                            // this just means template is already there.
                            // In the case other thread creates the template, we need to fill the template name
                            // used by the Tags.
                            e.printStackTrace();
                            if (tagTemplate != null && tagTemplate.getName() != null && tagTemplate.getName().trim().equals("")) {
                                tagTemplate = TagTemplate.newBuilder().mergeFrom(tagTemplate).
                                        setName(tagTemplateName).build();
                            }
                        }
                    }

                    // -------------------------------
                    // Lookup Data Catalog's Entry referring to the table.
                    // -------------------------------
                    String linkedResource =
                            String.format(
                                    "//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s",
                                    projectId, datasetId, tableName);
                    LookupEntryRequest lookupEntryRequest =
                            LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();

                    Object cachedEntry = dataCatalogLocalCache.getIfPresent(linkedResource);

                    com.google.cloud.datacatalog.v1beta1.Entry tableEntry = null;

                    if (cachedEntry != null) {
                        tableEntry = (com.google.cloud.datacatalog.v1beta1.Entry) cachedEntry;
                        if (VERBOSE_OUTPUT) {
                            System.out.println("[CacheHit] - lookupEntry");
                        }
                    } else {
                        tableEntry = dataCatalogClient.lookupEntry(lookupEntryRequest);
                        dataCatalogLocalCache.put(linkedResource, tableEntry);
                        if (VERBOSE_OUTPUT) {
                            System.out.println("[CacheMiss] - lookupEntry");
                        }
                    }

                    // -------------------------------
                    // Attach a Tag to the table.
                    // -------------------------------
                    GoogleCloudDatacatalogV1beta1Tag tag = new GoogleCloudDatacatalogV1beta1Tag();
                    tag.setTemplate(tagTemplate.getName());
                    Map<String, GoogleCloudDatacatalogV1beta1TagField> fields = new HashMap<>();

                    fields.put(
                            "top_info_type",
                            new GoogleCloudDatacatalogV1beta1TagField().setStringValue(topInfoType));
                    fields.put(
                            "info_types_detail",
                            new GoogleCloudDatacatalogV1beta1TagField()
                                    .setStringValue(getInfoTypeDetail(tempCol)));
                    fields.put(
                            "last_updated",
                            new GoogleCloudDatacatalogV1beta1TagField()
                                    .setStringValue(new java.util.Date().toString()));

                    String runID = UUID.randomUUID().toString();

                    fields.put("run_id", new GoogleCloudDatacatalogV1beta1TagField().setStringValue(runID));

                    tag.setFields(fields);
                    tag.setColumn(columnName);

                    // if create tag fails this likely means it already exists
                    // Note currently API requires iterating through tags to find the right one
                    String entryName = tableEntry.getName();
                    DataCatalogClient.ListTagsPagedResponse listTags = null;

                    Object cachedListTags = dataCatalogLocalCache.getIfPresent(entryName);

                    if (cachedListTags != null) {
                        listTags = (DataCatalogClient.ListTagsPagedResponse) cachedListTags;
                        if (VERBOSE_OUTPUT) {
                            System.out.println("[CacheHit] - listTags");
                        }
                    } else {
                        listTags = dataCatalogClient.listTags(entryName);
                        dataCatalogLocalCache.put(entryName, listTags);
                        if (VERBOSE_OUTPUT) {
                            System.out.println("[CacheMiss] - listTags");
                        }
                    }

                    Optional<Tag> existentTag = Optional.empty();

                    for (DataCatalogClient.ListTagsPage pagedResponse : listTags.iteratePages()) {
                        Predicate<Tag> tagEqualsPredicate =
                                currentTag -> currentTag.getTemplate().equalsIgnoreCase(tag.getTemplate());

                        Predicate<Tag> columnEqualsPredicate =
                                currentTag -> currentTag.getColumn().equalsIgnoreCase(tag.getColumn());

                        existentTag =
                                pagedResponse.getResponse().getTagsList().stream()
                                        .parallel()
                                        .filter(tagEqualsPredicate.and(columnEqualsPredicate))
                                        .findAny();

                        if (existentTag.isPresent()) {
                            break;
                        }
                    }

                    // -------------------------------------------
                    // Create Batch request for Patch/Create Tag.
                    // -------------------------------------------
                    if (existentTag.isPresent()) {
                        if (VERBOSE_OUTPUT) {
                            System.out.print("Found existing tag template in this column...[Updating]...");
                        } else {
                            System.out.print(".");
                        }
                        dataCatalogBatchClient
                                .projects()
                                .locations()
                                .entryGroups()
                                .entries()
                                .tags()
                                .patch(existentTag.get().getName(), tag)
                                .queue(batchRequest, DataCatalogBatchClient.updatedTagCallback);
                    } else {
                        dataCatalogBatchClient
                                .projects()
                                .locations()
                                .entryGroups()
                                .entries()
                                .tags()
                                .create(entryName, tag)
                                .queue(batchRequest, DataCatalogBatchClient.createdTagCallback);
                    }

                } catch (Exception err) {
                    System.out.println("Error writing findings to DC ");
                    err.printStackTrace();
                }
            }
        }
        try {
            batchRequest.execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Scan completed!!");
        return "Successful";
    }

    private static int getTopInfoTypeMin(java.util.Hashtable<String, Integer> tempCol) {
        String maxInfoTypeName = null;
        int maxInt = 0;
        for (String infoTypeName : tempCol.keySet()) {
            if (maxInfoTypeName == null || tempCol.get(infoTypeName) > maxInt) {
                maxInfoTypeName = infoTypeName;
                maxInt = tempCol.get(infoTypeName);
            }
        }
        return maxInt;
    }

    private static String getTopInfoType(java.util.Hashtable<String, Integer> tempCol) {
        String maxInfoTypeName = null;
        int maxInt = 0;
        for (String infoTypeName : tempCol.keySet()) {
            if (maxInfoTypeName == null || tempCol.get(infoTypeName) > maxInt) {
                maxInfoTypeName = infoTypeName;
                maxInt = tempCol.get(infoTypeName);
            }
        }
        return maxInfoTypeName;
    }

    private static String getInfoTypeDetail(java.util.Hashtable<String, Integer> tempCol) {
        String infoTypeDetails = null;
        for (String infoTypeName : tempCol.keySet()) {
            if (infoTypeDetails == null) {
                infoTypeDetails = infoTypeName + "[" + tempCol.get(infoTypeName) + "]";
            } else {
                infoTypeDetails =
                        infoTypeDetails + ", " + infoTypeName + "[" + tempCol.get(infoTypeName) + "]";
            }
        }
        return infoTypeDetails;
    }
}
