package com.iot.data.databeans;

import java.io.Serializable;

public class EventDataBean implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// uuid for packet
	private String iotRawDataRowKey;
	private String iotRawDataPacketId;
	private String iotRawDataAppSecret;
	private String iotRawDataDeviceId;
	private String iotRawDataServerTime;
	private String iotRawDataUniqId;
	private String eventName;
	private long timeStamp;
	private String dateStr;
	
	// iotRawDataRowKey get set methods
	public String getiotRawDataRowKey() {
		return iotRawDataRowKey;
	}
	public void setiotRawDataRowKey(String iotRawDataRowKey) {
		this.iotRawDataRowKey = iotRawDataRowKey;
	}
	
	// iotRawDataPacketId get set methods
	public String getiotRawDataPacketId() {
		return iotRawDataPacketId;
	}
	public void setiotRawDataPacketId(String iotRawDataPacketId) {
		this.iotRawDataPacketId = iotRawDataPacketId;
	}
	
	// iotRawDataAppSecret get set methods
	public String getiotRawDataAppSecret() {
		return iotRawDataAppSecret;
	}
	public void setiotRawDataAppSecret(String iotRawDataAppSecret) {
		this.iotRawDataAppSecret = iotRawDataAppSecret;
	}
	
	
	// iotRawDataDeviceId get set methods
	public String getiotRawDataDeviceId() {
		return iotRawDataDeviceId;
	}
	public void setiotRawDataDeviceId(String iotRawDataDeviceId) {
		this.iotRawDataDeviceId = iotRawDataDeviceId;
	}
	
	
	// iotRawDataServerTime get set methods
	public String getiotRawDataServerTime() {
		return iotRawDataServerTime;
	}
	public void setiotRawDataServerTime(String iotRawDataServerTime) {
		this.iotRawDataServerTime = iotRawDataServerTime;
	}
	
	// iotRawDataUniqId get set methods
	public String getiotRawDataUniqId() {
		return iotRawDataUniqId;
	}
	public void setiotRawDataUniqId(String iotRawDataUniqId) {
		this.iotRawDataUniqId = iotRawDataUniqId;
	}

	// timeStamp get set methods
	public long gettimeStamp() {
		return timeStamp;
	}
	public void settimeStamp(Long timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	// iotRawDataRowKey get set methods
	public String geteventName() {
		return eventName;
	}
	public void seteventName(String eventName) {
		this.eventName = eventName;
	}
	
	// dateStr get set methods
	public String getdateStr() {
		return dateStr;
	}
	public void setdateStr(String dateStr) {
		this.dateStr = dateStr;
	}
}