package com.iot.data.databeans;

import java.io.Serializable;

public class ESDataBean implements Serializable {
	private String iotRawDataRowKey;
	private String iotRawDataAppSecret;
	private String iotRawDataDeviceId;
	private String iotRawDataDeviceModel;
	private long timestamp;
	private String date;
	private String year;
	private String month;
	private String day;
	
	public String getiotRawDataRowKey() {
		return iotRawDataRowKey;
	}
	
	public void setiotRawDataRowKey(String iotRawDataRowKey) {
		this.iotRawDataRowKey = iotRawDataRowKey;
	}
	
	public String getiotRawDataAppSecret() {
		return iotRawDataAppSecret;
	}
	
	public void setiotRawDataAppSecret(String iotRawDataAppSecret) {
		this.iotRawDataAppSecret = iotRawDataAppSecret;
	}
	
	public String getiotRawDataDeviceId() {
		return iotRawDataDeviceId;
	}
	
	public void setiotRawDataDeviceId(String iotRawDataDeviceId) {
		this.iotRawDataDeviceId = iotRawDataDeviceId;
	}
	
	public String getiotRawDataDeviceModel() {
		return iotRawDataDeviceModel;
	}
	
	public void setiotRawDataDeviceModel(String iotRawDataDeviceModel) {
		this.iotRawDataDeviceModel = iotRawDataDeviceModel;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public String getDate() {
		return date;
	}
	
	public void setDate(String date) {
		this.date = date;
	}
	
}
