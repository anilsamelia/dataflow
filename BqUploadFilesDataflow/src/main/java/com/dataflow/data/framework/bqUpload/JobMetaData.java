package com.dataflow.data.framework.bqUpload;

import java.io.Serializable;

/**
 * This class hold is table-mapping data
 * 
 * @author Anil.Kumar
 * @version 1.0
 * @since 2022-02-08
 */
public class JobMetaData implements Serializable {

	private static final long serialVersionUID = 1L;

	private String datasetName;

	private String tableName;

	private String inputFilePattern;

	private Integer skipheaderRow;

	private String columnDelimiter;

	private String projectId;

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getInputFilePattern() {
		return inputFilePattern;
	}

	public void setInputFilePattern(String inputFilePattern) {
		this.inputFilePattern = inputFilePattern;
	}

	public Integer getSkipheaderRow() {
		return skipheaderRow;
	}

	public void setSkipheaderRow(Integer skipheaderRow) {
		this.skipheaderRow = skipheaderRow;
	}

	public String getColumnDelimiter() {
		return columnDelimiter;
	}

	public void setColumnDelimiter(String columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	@Override
	public String toString() {
		return "JobMetaData [datasetName=" + datasetName + ", tableName=" + tableName + ", inputFilePattern="
				+ inputFilePattern + ", skipheaderRow=" + skipheaderRow + ", columnDelimiter=" + columnDelimiter
				+ ", projectName=" + projectId + "]";
	}

}
