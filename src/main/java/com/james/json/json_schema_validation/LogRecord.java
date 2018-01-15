package com.james.json.json_schema_validation;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * LogRecord holds the data object stored in hazelcast queue before inserting actual data to hdfs
 * 
 * @author asagri
 * 
 */
public class LogRecord {
	private final static Logger LOG = Logger.getLogger(LogRecord.class);
	public static final String APP_ID_KEY = "logshed_app_id";
	public static final String OLD_APP_ID_KEY = "appid";
	public static final String CLIENT_ADDRESS_KEY = "client_address";
	public static final String LOG_TIME_KEY = "slogtime";
	public static final String LOG_PAYLOAD_KEY = "payload";
	public static final String LOG_PAYLOAD_TYPE_KEY = "type";
	public static final int LOG_PAYLOAD_TYPE_JSON = 1;
	public static final int LOG_PAYLOAD_TYPE_RAW = 2;

	private String appId;
	private String clientAddress;
	private long logTime;
	private String payloadStr;
	private JSONObject payloadJSON;
	private int payloadType;
	
	//represents the whole logrecord.Used to cache so that we dont create the JSONObject multiple times.
	private JSONObject logRecordJson = null;


	public LogRecord (String appId, long logTime, String payloadStr, String clientAddress){
		this.appId = appId;
		this.clientAddress = clientAddress;
		this.logTime = logTime;
		this.payloadStr = payloadStr;
		this.payloadType = LOG_PAYLOAD_TYPE_RAW;
	}

	public LogRecord (String appId, long logTime, JSONObject payloadJSON, String clientAddress){
		this.appId = appId;
		this.clientAddress = clientAddress;
		this.logTime = logTime;
		this.payloadJSON = payloadJSON;
		this.payloadType = LOG_PAYLOAD_TYPE_JSON;
	}

	public LogRecord (byte[] recordBytes){ //de-serialization
		try {
			JSONObject logJSON = new JSONObject(new String(recordBytes));
			
			this.appId = logJSON.optString(APP_ID_KEY);
			if (StringUtils.isEmpty(this.appId)) {
				this.appId = logJSON.optString(OLD_APP_ID_KEY);
			}
			
			this.clientAddress = logJSON.optString(CLIENT_ADDRESS_KEY);
			this.logTime = logJSON.getLong(LOG_TIME_KEY);
			this.payloadType = logJSON.getInt(LOG_PAYLOAD_TYPE_KEY);
			
			if (this.payloadType == LOG_PAYLOAD_TYPE_RAW){
				this.payloadStr = logJSON.getString(LOG_PAYLOAD_KEY);
			}else{
				this.payloadJSON = logJSON.getJSONObject(LOG_PAYLOAD_KEY);
			}
			
			this.logRecordJson = logJSON;
		} catch (JSONException e) {
			LOG.error(e);
		}
	}

	public String getAppId() {
		return appId;
	}

	public long getLogTime() {
		return logTime;
	}

	public String getPayload() {
		if (this.payloadType == LOG_PAYLOAD_TYPE_RAW){
			return payloadStr;
		}else{
			return payloadJSON.toString();
		}
	}
	
	public int getPayloadType() {
		return payloadType;
	}

	/**
	 * This is a wrapper method to return server logtime as well as payload to store in HDFS. This is based on request from MIS team. They want to know
	 * when Logshed Collector received a message. We only wrap JSON logs with server log time. Raw messages are still stored without any wrapper.
	 */
	public String getHDFSPayload() {
		if (this.payloadType == LOG_PAYLOAD_TYPE_RAW){
			return payloadStr;
		}else{
			JSONObject hdfsPayload = createJSONObj(false); //No need for old app id as this payload is used for storing in S3.(not for hazelcast)
			return hdfsPayload.toString();
		}
	}


	@Override
	public String toString() {
		return "LogRecord [appId=" + appId + ", clientAddress=" + clientAddress + ", logTime=" + logTime
				+ ", payloadType=" + payloadType + "]";
	}

	public String toJSONString() {
		JSONObject jsonObj = this.toJSONObj();
		return (jsonObj == null) ? "" : jsonObj.toString();
	}
	
	/**
	 * This method is used for serialization of LogRecord to store in Hazelcast. 
	 * New logshed code will be released on logshed machines one by one. Hence 
	 * some instances that are processing hazelcast queue would be stil the old release.
	 * Hence we need to have both old appid key (appid) and new app id key (logshed_app_id) 
	 * in the json to avoid de-serialization errors. 
	 * @return
	 */
	public JSONObject toJSONObj() {
		if (logRecordJson == null) {
			logRecordJson = createJSONObj(false); //Changed it to false to remove the appid as a part of Logrecords
		}
		
		return logRecordJson;
	}

	/**
	 *  this is temporary until the next release when all events in Hazelcast will have new data structure of the LogRecord.
	 * 
	 */
	private JSONObject createJSONObj(boolean includeOldAppId) {
		JSONObject logRecordJson = new JSONObject();
		try {
			logRecordJson.put(APP_ID_KEY, appId);
			if (includeOldAppId) {
				logRecordJson.put(OLD_APP_ID_KEY, appId);
			}
			logRecordJson.put(LOG_TIME_KEY, logTime);
			logRecordJson.put(CLIENT_ADDRESS_KEY, StringUtils.defaultString(clientAddress));			
			logRecordJson.put(LOG_PAYLOAD_TYPE_KEY, payloadType);
			if (this.payloadType == LOG_PAYLOAD_TYPE_RAW){
				logRecordJson.put(LOG_PAYLOAD_KEY, payloadStr);
			}else{
				logRecordJson.put(LOG_PAYLOAD_KEY, payloadJSON);
			}
		} catch (JSONException e) {
			LOG.error(e);
		}
		return logRecordJson;
	}
	
	public byte[] getBytes() {
		return this.toJSONString().getBytes();
	}
}
