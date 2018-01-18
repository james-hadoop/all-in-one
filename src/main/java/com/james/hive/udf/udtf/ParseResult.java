package com.james.hive.udf.udtf;

import java.util.Collection;

import org.json.JSONObject;

public class ParseResult {
	private Collection<LogRecord> logRecords;
	private JSONObject errorMessage;
	private int validationFailedRecordsCount;
	private int sizeFailedRecordsCount;
	
	public ParseResult(Collection<LogRecord> logRecords, int validationFailedRecordsCount,int sizeFailedRecordsCount,
			JSONObject errorMessage) {
		super();
		this.logRecords = logRecords;
		this.validationFailedRecordsCount = validationFailedRecordsCount;
		this.sizeFailedRecordsCount = sizeFailedRecordsCount;
		this.errorMessage = errorMessage;
	}

	public Collection<LogRecord> getLogRecords() {
		return logRecords;
	}

	public JSONObject getErrorMessage() {
		return errorMessage;
	}

	public int getValidationFailedRecordsCount() {
		return this.validationFailedRecordsCount;
	}

	public int getSizeFailedRecordsCount() {
		return sizeFailedRecordsCount;
	}
	
}
