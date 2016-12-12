package com.iot.data.databeans;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class IotRawDataBean implements Serializable{
	// uuid for packet
	private String iotRawDataRowKey;
	private String iotRawDataPacketId;
	private String iotRawDataAppSecret;
	private String iotRawDataDeviceId;
	private String iotRawDataServerTime;
	private String iotRawDataIMEI;
	private String iotRawDataWFMac;
	private String iotRawDataUniqId;
	private String iotRawDataEventName;
	private String iotRawDataEventProperties;
	// push action data
	private String iotRawDataPUSHACTIONPUSHKEY;
	// advertising action data
	private String iotRawDataAdvertisingIdActionADVERTISINGIDKEY;
	
	//device level data
	//private String iotRawDataDeviceModel;
	
	private String date;
	private String year;
	private String month;
	private String day;

	// date, time stuff
	private long timeStamp;
	private String dateStr;
	// event stuff
	private String eventMapDet;
	
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
	
	// iotRawDataPUSHACTIONPUSHKEY get set methods
	public String getiotRawDataPUSHACTIONPUSHKEY() {
		return iotRawDataPUSHACTIONPUSHKEY;
	}
	public void setiotRawDataPUSHACTIONPUSHKEY(String iotRawDataPUSHACTIONPUSHKEY) {
		this.iotRawDataPUSHACTIONPUSHKEY = iotRawDataPUSHACTIONPUSHKEY;
	}
	
	// iotRawDataAdvertisingIdActionADVERTISINGIDKEY get set methods
	public String getiotRawDataAdvertisingIdActionADVERTISINGIDKEY() {
		return iotRawDataAdvertisingIdActionADVERTISINGIDKEY;
	}
	public void setiotRawDataAdvertisingIdActionADVERTISINGIDKEY(String iotRawDataAdvertisingIdActionADVERTISINGIDKEY) {
		this.iotRawDataAdvertisingIdActionADVERTISINGIDKEY = iotRawDataAdvertisingIdActionADVERTISINGIDKEY;
	}
	
	// iotRawDataIMEI get set methods
	public String getiotRawDataIMEI() {
		return iotRawDataIMEI;
	}
	public void setiotRawDataIMEI(String iotRawDataIMEI) {
		this.iotRawDataIMEI = iotRawDataIMEI;
	}
	
	// iotRawDataWFMac get set methods
	public String getiotRawDataWFMac() {
		return iotRawDataWFMac;
	}
	public void setiotRawDataWFMac(String iotRawDataWFMac) {
		this.iotRawDataWFMac = iotRawDataWFMac;
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
	public void settimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	//date set get methods
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	
	//year set get methods
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	
	//month set get methods
	public String getMonth() {
		return month;
	}
	public void setMonth(String month) {
		this.month = month;
	}
	
	//day set get methods
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	// iotRawDataRowKey get set methods
	public String geteventMapDet() {
		return eventMapDet;
	}
	public void seteventMapDet(String eventMapDet) {
		this.eventMapDet = eventMapDet;
	}
	
	// dateStr get set methods
	public String getdateStr() {
		return dateStr;
	}
	public void setdateStr(String dateStr) {
		this.dateStr = dateStr;
	}
	
	//event name get set methods
	public String getiotRawDataEventName() {
		return iotRawDataEventName;
	}
	public void setiotRawDataEventName(String iotRawDataEventName) {
		this.iotRawDataEventName = iotRawDataEventName;
	}
	
	//event properties get set methods
	public String getiotRawDataEventProperties() {
		return iotRawDataEventProperties;
	}
	public void setiotRawDataEventProperties(String iotRawDataEventProperties) {
		this.iotRawDataEventProperties = iotRawDataEventProperties;
	}
	
	//device model properties get set methods
	/*public String getiotRawDataDeviceModel() {
		return iotRawDataDeviceModel;
	}
	public void setiotRawDataDeviceModel(String iotRawDataDeviceModel) {
		this.iotRawDataDeviceModel = iotRawDataDeviceModel;
	}*/
	
}