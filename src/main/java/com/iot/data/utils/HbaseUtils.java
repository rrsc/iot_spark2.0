package com.iot.data.utils;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.iot.data.databeans.ESDataBean;
import com.iot.data.databeans.IotRawDataBean;
import com.iot.data.queries.TheInThings2;
import com.iot.data.schema.PacketData;
import com.iot.data.schema.SerializableIotData;

public class HbaseUtils implements Serializable {

	private String confFileName;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	private static ESDataBean esbean;
	private static IotRawDataBean iot;
	
	// hbase params
	private static String raw_iot_tab_colfam;
	private static String iot_secondary;
	private static String iot_secondary_colfam;
	private static String push_adid_dev_map_tab_cf;
	private static String push_appsecret_dev_tab_cf;
	private static String AUpreprocess, AUpreprocess_cf, NUpreprocess, NUpreprocess_cf;
	private static String active_users_tab_cf;
	private static String eventDB_event_count_tab_cf;
	private static String hbaseTableEvents1;
	private static String hbaseTableEvents1_cf;
	
	private static String hbaseTableEvents2_cf;
	private static String hbaseTableEventsAU_cf;
	private static String hbaseTableEventsNU_cf;
	
	private static String new_users_tab_cf;
	private static String hbase_master_ip;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	
	private static Configuration hbaseIotConf;
//	private static Put primaryPut, p, activePreprocessPut, newPreprocessPut, AUPut, NUPut, eventsPut, eventsAUPut, eventsNUPut, pushDataPut, adIdPut;
	//private static Put eventDBPut, pushDevIdPut;
	private static HTable secondaryTable;
	
	private final static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
	
	public HbaseUtils(String confTypeFile) throws IOException, ParseException {
		// read conf file and corresponding params
		confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
		//confFileName = "/Users/sahil/Desktop/conf/" + confTypeFile;
		// read the json file and create a map of the parameters
		confObj = confParser.parse(new FileReader(confFileName));
        confJsonObject = (JSONObject) confObj;
        // read parameters from conf file
        
        // hbase params
        iot_secondary = (String) confJsonObject.get("hbase_table_secondary");
        
        raw_iot_tab_colfam = (String) confJsonObject.get("hbase_raw_data_tab_colfam");
        iot_secondary_colfam = (String) confJsonObject.get("hbase_table_secondary_colfam");
        
        push_adid_dev_map_tab_cf = (String) confJsonObject.get("hbase_dev_map_apps_tab_colfam");       
        push_appsecret_dev_tab_cf = (String) confJsonObject.get("hbase_dev_push_apps_colfam");
        
        AUpreprocess = (String) confJsonObject.get("hbase_activeUsers_preprocess");
        AUpreprocess_cf = (String) confJsonObject.get("hbase_activeUsers_preprocess_colfam");       
        NUpreprocess = (String) confJsonObject.get("hbase_newUsers_preprocess");
        NUpreprocess_cf = (String) confJsonObject.get("hbase_newUsers_preprocess_colfam");
        
        active_users_tab_cf = (String) confJsonObject.get("hbase_activeUsers_colfam"); 
        
        new_users_tab_cf = (String) confJsonObject.get("hbase_newUsers_colfam");

        eventDB_event_count_tab_cf = (String) confJsonObject.get("hbase_eventsDBevents_colfam");
        
        hbaseTableEvents1 = (String) confJsonObject.get("hbase_table_events1");
        hbaseTableEvents1_cf = (String) confJsonObject.get("hbase_table_events1_colfam");
        
        hbaseTableEvents2_cf = (String) confJsonObject.get("hbase_table_events2_colfam");
        hbaseTableEventsAU_cf = (String) confJsonObject.get("hbase_table_events_AU_colfam");
        hbaseTableEventsNU_cf = (String) confJsonObject.get("hbase_table_events_NU_colfam");

        hbase_master_ip = (String) confJsonObject.get("server_ip");
        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
        hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
        
        // set up hbase conn
        hbaseIotConf = HBaseConfiguration.create();
        hbaseIotConf.set("hbase.master",hbase_master_ip + ":" + hbase_master_port);
        hbaseIotConf.set("hbase.zookeeper.quorum", hbase_master_ip);
        hbaseIotConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);	
        
        secondaryTable = new HTable(hbaseIotConf, iot_secondary);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtIotRawDataToPut(SerializableIotData iotRawPacket) {
		// put the data
		String currIotRowKey = iotRawPacket.getAppSecret() + "__" + iotRawPacket.getPacketId();
		Put primaryPut = new Put(Bytes.toBytes(currIotRowKey));
		//p = new Put(Bytes.toBytes(currIotRowKey));
		// add basic data to corresponding columns
		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("packet_id"), Bytes.toBytes(iotRawPacket.getPacketId()));
		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(iotRawPacket.getAppSecret()));
		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("device_id"), Bytes.toBytes(iotRawPacket.getDeviceId()));
		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("library"), Bytes.toBytes(iotRawPacket.getLibrary()));
		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("library_version"), Bytes.toBytes(iotRawPacket.getLibraryVersion()));
		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("server_ip"), Bytes.toBytes(iotRawPacket.getServerIp()));
		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("server_time"), Bytes.toBytes(iotRawPacket.getServerTime()));
		// get unique_id_action in a hashmap and put the data
	    Map<String,String> currUidDataMap = new HashMap<String,String>();
	    currUidDataMap = iotRawPacket.getUNIQUEIDACTION();
	        	for (Map.Entry<String, String> entry : currUidDataMap.entrySet()) {
	        		primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
							Bytes.toBytes("UID__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
	        	}
	        	// Loop through "packet" and put the data
	        	// setup a few counts
	        	int countScreens = 0;
	        	int countEvents = 0;

	        	Map<String,HashMap<String,String>> finEventDataMap = new HashMap<String,HashMap<String,String>>();
	        	for (PacketData currPd : iotRawPacket.getPacket()) {
	        		// Install Referrer Data
	        		if ( currPd.getInstallReferrer() != null ){
	        			primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasIRdata"), Bytes.toBytes(true));
	        			if ( currPd.getInstallReferrer().getAction() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRAction"), Bytes.toBytes(currPd.getInstallReferrer().getAction()));
	        			}
	        			if ( currPd.getInstallReferrer().getAppSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRAppSessionID"), Bytes.toBytes(currPd.getInstallReferrer().getAppSessionId()));
	        			}
	        			if ( currPd.getInstallReferrer().getReferrerString() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRRefString"), Bytes.toBytes(currPd.getInstallReferrer().getReferrerString()));
	        			}
	        			if ( currPd.getInstallReferrer().getTimestamp() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRTimestamp"), Bytes.toBytes(currPd.getInstallReferrer().getTimestamp()));
	        			}
	        			if ( currPd.getInstallReferrer().getUserId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("IRUserId"), Bytes.toBytes(currPd.getInstallReferrer().getUserId()));
	        			}
	    			}
	        		// start session data
	        		if (currPd.getStartSession() != null){
	        			primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasStartSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStartSession().getAction() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnAction"), Bytes.toBytes(currPd.getStartSession().getAction()));
	        			}
	        			if ( currPd.getStartSession().getAppSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnAppSessionID"), Bytes.toBytes(currPd.getStartSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStartSession().getScreenName() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnScreenName"), Bytes.toBytes(currPd.getStartSession().getScreenName()));
	        			}
	        			if ( currPd.getStartSession().getTimestamp() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnTimestamp"), Bytes.toBytes(currPd.getStartSession().getTimestamp()));
	        			}
	        			if ( currPd.getStartSession().getSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StartSessnSessionID"), Bytes.toBytes(currPd.getStartSession().getSessionId()));
	        			}
	    			}
	        		// stop session data
	        		if ( currPd.getStopSession() != null ){
	        			primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasStopSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStopSession().getAction() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnAction"), Bytes.toBytes(currPd.getStopSession().getAction()));
	        			}
	        			if ( currPd.getStopSession().getAppSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnAppSessionID"), Bytes.toBytes(currPd.getStopSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStopSession().getDuration() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnDuration"), Bytes.toBytes(currPd.getStopSession().getDuration()));
	        			}
	        			if ( currPd.getStopSession().getTimestamp() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnTimestamp"), Bytes.toBytes(currPd.getStopSession().getTimestamp()));
	        			}
	        			if ( currPd.getStopSession().getSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnSessionID"), Bytes.toBytes(currPd.getStopSession().getSessionId()));
	        			}
	        			if ( currPd.getStopSession().getUserId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("StopSessnUserID"), Bytes.toBytes(currPd.getStopSession().getUserId()));
	        			}
	    			}
	        		// screen data
                    if (currPd.getScreen() != null){
                    	// increment screen count and set up hbase cols according to count
                    	countScreens += 1;
                    	String curScreenCntStr = "__" + Integer.toString(countScreens);
                    	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasScreen"), Bytes.toBytes(true));
                        if ( currPd.getScreen().getAction() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenAction" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAction()));
                        }
                        if ( currPd.getScreen().getAppSessionId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenAppSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAppSessionId()));
                        }
                        if ( currPd.getScreen().getTimestamp() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenTimeStamp" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getTimestamp()));
                        }
                        if ( currPd.getScreen().getSessionId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getSessionId()));
                        }
                        if ( currPd.getScreen().getScreenId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("ScreenScreenID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getScreenId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getScreen().getProperties() != null ){
                            Map<String,String> currScreenMap = new HashMap<String,String>();
                            currScreenMap = currPd.getScreen().getProperties();
                            primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasScreenProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currScreenMap.entrySet())
                            {
                            	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("ScreenProp__" + curScreenCntStr + "__" + entry.getKey()),
                                        	Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // identity data
                    if (currPd.getIdentity() != null){
                    	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasID"), Bytes.toBytes(true));
                        if ( currPd.getIdentity().getAction() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDAction"), Bytes.toBytes(currPd.getIdentity().getAction()));
                        }
                        if ( currPd.getIdentity().getAppSessionId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDAppSessionID"), Bytes.toBytes(currPd.getIdentity().getAppSessionId()));
                        }
                        if ( currPd.getIdentity().getTimestamp() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDTimeStamp"), Bytes.toBytes(currPd.getIdentity().getTimestamp()));
                        }
                        if ( currPd.getIdentity().getSessionId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDSessionID"), Bytes.toBytes(currPd.getIdentity().getSessionId()));
                        }
                        if ( currPd.getIdentity().getUserId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("IDUserID"), Bytes.toBytes(currPd.getIdentity().getUserId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getIdentity().getProperties() != null ){
                            Map<String,String> currIdentityMap = new HashMap<String,String>();
                            currIdentityMap = currPd.getIdentity().getProperties();
                            primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasIDProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currIdentityMap.entrySet())
                            {
                            	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("IDProp__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // events data
                    if (currPd.getEvents() != null){
                    	// increment events count and set up hbase cols according to count
                    	countEvents += 1;
                    	String curEventCntStr = "__" + Integer.toString(countEvents);
                    	// store the details in a map
                    	HashMap<String, String> currEventDataMap = new HashMap<String,String>();
                    	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                Bytes.toBytes("hasEvents"), Bytes.toBytes(true));
                        if ( currPd.getEvents().getAction() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsAction" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAction()));
                            currEventDataMap.put("EventsAction", currPd.getEvents().getAction());
                        }
                        if ( currPd.getEvents().getAppSessionId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsAppSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAppSessionId()));
                            currEventDataMap.put("EventsAppSessionID", currPd.getEvents().getAppSessionId());
                        }
                        if ( currPd.getEvents().getTimestamp() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsTimeStamp" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getTimestamp()));
                            currEventDataMap.put("EventsTimeStamp", currPd.getEvents().getTimestamp());
                        }
                        if ( currPd.getEvents().getSessionId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getSessionId()));
                            currEventDataMap.put("EventsSessionID", currPd.getEvents().getSessionId());
                        }
                        if ( currPd.getEvents().getUserId() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsUserID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getUserId()));
                            currEventDataMap.put("EventsUserID", currPd.getEvents().getUserId());
                        }
                        if ( currPd.getEvents().getEvent() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("Events" + curEventCntStr),
                                    Bytes.toBytes(currPd.getEvents().getEvent()));
                            currEventDataMap.put("Event", currPd.getEvents().getEvent());
                        }
                        if ( currPd.getEvents().getPosition() != null ){
                        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("EventsPosition"), Bytes.toBytes(currPd.getEvents().getPosition()));
                            currEventDataMap.put("EventsPosition", Long.toString( currPd.getEvents().getPosition() ));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getEvents().getProperties() != null ){
                            Map<String,String> currEventsMap = new HashMap<String,String>();
                            currEventsMap = currPd.getEvents().getProperties();
                            primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                    Bytes.toBytes("hasEventsProps"), Bytes.toBytes(true));
                            currEventDataMap.put("EventsProp", currPd.getEvents().getProperties().toString());
                            for (Map.Entry<String, String> entry : currEventsMap.entrySet())
                            {
                            	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
                                        Bytes.toBytes("EventsProp"+ curEventCntStr + "__" + entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                        finEventDataMap.put("Event" + curEventCntStr, currEventDataMap);
                    }
	        		// push action data
	        		if (currPd.getPUSHACTION() != null){
	        			primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasPushActn"), Bytes.toBytes(true));
	        			if ( currPd.getPUSHACTION().getAction() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnActionName"), Bytes.toBytes(currPd.getPUSHACTION().getAction()));
	        			}
	        			if ( currPd.getPUSHACTION().getAppSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnAppSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getPUSHKEY() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnPushKey"), Bytes.toBytes(currPd.getPUSHACTION().getPUSHKEY()));
	        			}
	        			if ( currPd.getPUSHACTION().getTimestamp() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnTimestamp"), Bytes.toBytes(currPd.getPUSHACTION().getTimestamp()));
	        			}
	        			if ( currPd.getPUSHACTION().getSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getUserId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("PushActnUserID"), Bytes.toBytes(currPd.getPUSHACTION().getUserId()));
	        			}
	    			}
	        		// adv_id action data
	        		if (currPd.getADVERTISINGIDACTION() != null){
	        			primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasAdIDAction"), Bytes.toBytes(true));
	        			if ( currPd.getADVERTISINGIDACTION().getAction() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionActionName"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAction()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getAppSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAppSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAdIDKey"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getTimestamp() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionTimestamp"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getTimestamp()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getSessionId() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("AdIDActionAdIDOptOut"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT()));
	        			}
	    			}
	        		// new device data
	        		if (currPd.getNewDevice() != null){
	        			primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasNewDev"), Bytes.toBytes(true));
	        			if ( currPd.getNewDevice().getAction() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("NewDevAction"), Bytes.toBytes(currPd.getNewDevice().getAction()));
	        			}
	        			if ( currPd.getNewDevice().getTimestamp() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("NewDevTimeStamp"), Bytes.toBytes(currPd.getNewDevice().getTimestamp()));
	        			}
	        			if ( currPd.getNewDevice().getContext() != null ){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("hasNewDevCxt"), Bytes.toBytes(true));
	        				// context features data
	        				if ( currPd.getNewDevice().getContext().getFeatures() != null ){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtFeatures"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasNFC() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtFeaturesNFC"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasNFC()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasTelephony() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtFeaturesTelephony"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasTelephony()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGPS() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGPS"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGPS()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesAcclroMtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasBarometer() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBaromtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasBarometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasCompass() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesCompass"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasCompass()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGyroscope() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGyro"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGyroscope()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasLightsensor() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesLightSensr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasLightsensor()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasProximity() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesProxmty"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasProximity()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBTVrsn"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion()));
	        					}
	        					
	        				}
	        				// context display data
	        				if ( currPd.getNewDevice().getContext().getDisplay() != null ){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtDisplay"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayHeight() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayHeight"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayHeight()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayWidth() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayWidth"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayWidth()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayDensity() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtDisplayDensity"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayDensity()));
	        					}
	        				}
	        				// context total mem info data
	        				if ( currPd.getNewDevice().getContext().getTotalMemoryInfo() != null ){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasNewDevCxtTotalMemry"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryRAM"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryStorage"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage()));
	        					}
	        				}
	        			}
	        			
	        		}
	        		// device info data
	        		if (currPd.getDeviceInfo() != null){
	        			primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
								Bytes.toBytes("hasDevInfo"), Bytes.toBytes(true));
	        			if (currPd.getDeviceInfo().getAction() != null){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoAction"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAction()));
	        			}
	        			if (currPd.getDeviceInfo().getTimestamp() != null){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoTimestamp"), 
									Bytes.toBytes(currPd.getDeviceInfo().getTimestamp()));
	        			}
	        			if (currPd.getDeviceInfo().getAppSessionId() != null){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoAppSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAppSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getSessionId() != null){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("DevInfoSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getContext() != null){
	        				primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
									Bytes.toBytes("hasDevInfoCxt"), Bytes.toBytes(true));
	        				if (currPd.getDeviceInfo().getContext().getAppBuild() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtAppBuild"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getPackageName() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildPackageName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getPackageName()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnCode"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionName() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionName()));
	        					}
	        				}
	        				// device info context device
	        				if (currPd.getDeviceInfo().getContext().getDevice() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtDevice"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getDevice().getSdkVersion() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceSDKVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getSdkVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceReleaseVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBrand"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceManfactrer"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceModel() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceModel"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceModel()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBoard"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtDeviceProduct"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct()));
	        					}
	        				}
	        				// device info context locale
	        				if (currPd.getDeviceInfo().getContext().getLocale() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtLocale"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevCountry"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevLang"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage()));
	        					}
	        				}
	        				// device info context location
	        				if (currPd.getDeviceInfo().getContext().getLocation() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtLocation"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLatitude() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationLat"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLatitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLongitude() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationLong"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLongitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getSpeed() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtLocationSpeed"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getSpeed()));
	        					}
	        				}
	        				// device info context telephone
	        				if (currPd.getDeviceInfo().getContext().getTelephone() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtTelephone"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnCarrier"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnRadio"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getInRoaming() != null){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtTelephoneInRoaming"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getInRoaming()));
	        					}
	        				}
	        				// device info context wifi
	        				if (currPd.getDeviceInfo().getContext().getWifi() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtWifi"), Bytes.toBytes(true));
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("DevInfoCxtWifi"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getWifi().toString()));
	        				}
	        				// device info context bluetoothInfo
	        				if (currPd.getDeviceInfo().getContext().getBluetoothInfo() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtBTInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtBTInfoBTStatus"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus()));
	        					}
	        				}
	        				// device info context availableMemoryInfo
	        				if (currPd.getDeviceInfo().getContext().getAvailableMemoryInfo() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtAvailbleMemryInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailRAM"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailStorage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage()));
	        					}
	        				}
	        				// device info context cpuInfo
	        				if (currPd.getDeviceInfo().getContext().getCpuInfo() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtCPUInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoTotal"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoIdle"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoUsage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage()));
	        					}
	        				}
	        				// device info context USER_AGENT_ACTION
	        				if (currPd.getDeviceInfo().getContext().getUSERAGENTACTION() != null){
	        					primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
										Bytes.toBytes("hasDevInfoCxtUsrAgntActn"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent() != null ){
	        						primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
											Bytes.toBytes("DevInfoCxtUsrAgntActnUsrAgnt"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent()));
	        					}
	        				}
	        			}
	        		}
	        	}
	        	
	        	primaryPut.add(Bytes.toBytes(raw_iot_tab_colfam),
						Bytes.toBytes("eventDataMap"), Bytes.toBytes(finEventDataMap.toString()));
	        	// return the put object
	        	return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currIotRowKey)), primaryPut);
	}
	
	
	@SuppressWarnings("deprecation")
	public void cnvrtIoTSecondaryDataToPut(SerializableIotData iotRawPacket) throws IOException {
		// put the data
		String currIotRowKey = iotRawPacket.getAppSecret() + "__" + iotRawPacket.getPacketId();
		Put p = new Put(Bytes.toBytes(currIotRowKey));
		//p = new Put(Bytes.toBytes(currIotRowKey));
		// add basic data to corresponding columns
		p.add(Bytes.toBytes(iot_secondary_colfam), Bytes.toBytes("packet_id"), Bytes.toBytes(iotRawPacket.getPacketId()));
		p.add(Bytes.toBytes(iot_secondary_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(iotRawPacket.getAppSecret()));
		p.add(Bytes.toBytes(iot_secondary_colfam), Bytes.toBytes("device_id"), Bytes.toBytes(iotRawPacket.getDeviceId()));
		p.add(Bytes.toBytes(iot_secondary_colfam), Bytes.toBytes("library"), Bytes.toBytes(iotRawPacket.getLibrary()));
		p.add(Bytes.toBytes(iot_secondary_colfam), Bytes.toBytes("library_version"), Bytes.toBytes(iotRawPacket.getLibraryVersion()));
		p.add(Bytes.toBytes(iot_secondary_colfam), Bytes.toBytes("server_ip"), Bytes.toBytes(iotRawPacket.getServerIp()));
		p.add(Bytes.toBytes(iot_secondary_colfam), Bytes.toBytes("server_time"), Bytes.toBytes(iotRawPacket.getServerTime()));
		// get unique_id_action in a hashmap and put the data
	    Map<String,String> currUidDataMap = new HashMap<String,String>();
	    currUidDataMap = iotRawPacket.getUNIQUEIDACTION();
	        	for (Map.Entry<String, String> entry : currUidDataMap.entrySet()) {
	        		p.add(Bytes.toBytes(iot_secondary_colfam),
							Bytes.toBytes("UID__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
	        	}
	        	// Loop through "packet" and put the data
	        	// setup a few counts
	        	int countScreens = 0;
	        	int countEvents = 0;

	        	Map<String,HashMap<String,String>> finEventDataMap = new HashMap<String,HashMap<String,String>>();
	        	for (PacketData currPd : iotRawPacket.getPacket()) {
	        		// Install Referrer Data
	        		if ( currPd.getInstallReferrer() != null ){
	        			p.add(Bytes.toBytes(iot_secondary_colfam),
								Bytes.toBytes("hasIRdata"), Bytes.toBytes(true));
	        			if ( currPd.getInstallReferrer().getAction() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("IRAction"), Bytes.toBytes(currPd.getInstallReferrer().getAction()));
	        			}
	        			if ( currPd.getInstallReferrer().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("IRAppSessionID"), Bytes.toBytes(currPd.getInstallReferrer().getAppSessionId()));
	        			}
	        			if ( currPd.getInstallReferrer().getReferrerString() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("IRRefString"), Bytes.toBytes(currPd.getInstallReferrer().getReferrerString()));
	        			}
	        			if ( currPd.getInstallReferrer().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("IRTimestamp"), Bytes.toBytes(currPd.getInstallReferrer().getTimestamp()));
	        			}
	        			if ( currPd.getInstallReferrer().getUserId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("IRUserId"), Bytes.toBytes(currPd.getInstallReferrer().getUserId()));
	        			}
	    			}
	        		// start session data
	        		if (currPd.getStartSession() != null){
	        			p.add(Bytes.toBytes(iot_secondary_colfam),
								Bytes.toBytes("hasStartSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStartSession().getAction() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StartSessnAction"), Bytes.toBytes(currPd.getStartSession().getAction()));
	        			}
	        			if ( currPd.getStartSession().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StartSessnAppSessionID"), Bytes.toBytes(currPd.getStartSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStartSession().getScreenName() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StartSessnScreenName"), Bytes.toBytes(currPd.getStartSession().getScreenName()));
	        			}
	        			if ( currPd.getStartSession().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StartSessnTimestamp"), Bytes.toBytes(currPd.getStartSession().getTimestamp()));
	        			}
	        			if ( currPd.getStartSession().getSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StartSessnSessionID"), Bytes.toBytes(currPd.getStartSession().getSessionId()));
	        			}
	    			}
	        		// stop session data
	        		if ( currPd.getStopSession() != null ){
	        			p.add(Bytes.toBytes(iot_secondary_colfam),
								Bytes.toBytes("hasStopSessn"), Bytes.toBytes(true));
	        			if ( currPd.getStopSession().getAction() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StopSessnAction"), Bytes.toBytes(currPd.getStopSession().getAction()));
	        			}
	        			if ( currPd.getStopSession().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StopSessnAppSessionID"), Bytes.toBytes(currPd.getStopSession().getAppSessionId()));
	        			}
	        			if ( currPd.getStopSession().getDuration() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StopSessnDuration"), Bytes.toBytes(currPd.getStopSession().getDuration()));
	        			}
	        			if ( currPd.getStopSession().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StopSessnTimestamp"), Bytes.toBytes(currPd.getStopSession().getTimestamp()));
	        			}
	        			if ( currPd.getStopSession().getSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StopSessnSessionID"), Bytes.toBytes(currPd.getStopSession().getSessionId()));
	        			}
	        			if ( currPd.getStopSession().getUserId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("StopSessnUserID"), Bytes.toBytes(currPd.getStopSession().getUserId()));
	        			}
	    			}
	        		// screen data
                    if (currPd.getScreen() != null){
                    	// increment screen count and set up hbase cols according to count
                    	countScreens += 1;
                    	String curScreenCntStr = "__" + Integer.toString(countScreens);
                        p.add(Bytes.toBytes(iot_secondary_colfam),
                                Bytes.toBytes("hasScreen"), Bytes.toBytes(true));
                        if ( currPd.getScreen().getAction() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("ScreenAction" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAction()));
                        }
                        if ( currPd.getScreen().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("ScreenAppSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getAppSessionId()));
                        }
                        if ( currPd.getScreen().getTimestamp() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("ScreenTimeStamp" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getTimestamp()));
                        }
                        if ( currPd.getScreen().getSessionId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("ScreenSessionID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getSessionId()));
                        }
                        if ( currPd.getScreen().getScreenId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("ScreenScreenID" + curScreenCntStr), Bytes.toBytes(currPd.getScreen().getScreenId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getScreen().getProperties() != null ){
                            Map<String,String> currScreenMap = new HashMap<String,String>();
                            currScreenMap = currPd.getScreen().getProperties();
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("hasScreenProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currScreenMap.entrySet())
                            {
                                p.add(Bytes.toBytes(iot_secondary_colfam),
                                        Bytes.toBytes("ScreenProp__" + curScreenCntStr + "__" + entry.getKey()),
                                        	Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // identity data
                    if (currPd.getIdentity() != null){
                        p.add(Bytes.toBytes(iot_secondary_colfam),
                                Bytes.toBytes("hasID"), Bytes.toBytes(true));
                        if ( currPd.getIdentity().getAction() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("IDAction"), Bytes.toBytes(currPd.getIdentity().getAction()));
                        }
                        if ( currPd.getIdentity().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("IDAppSessionID"), Bytes.toBytes(currPd.getIdentity().getAppSessionId()));
                        }
                        if ( currPd.getIdentity().getTimestamp() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("IDTimeStamp"), Bytes.toBytes(currPd.getIdentity().getTimestamp()));
                        }
                        if ( currPd.getIdentity().getSessionId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("IDSessionID"), Bytes.toBytes(currPd.getIdentity().getSessionId()));
                        }
                        if ( currPd.getIdentity().getUserId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("IDUserID"), Bytes.toBytes(currPd.getIdentity().getUserId()));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getIdentity().getProperties() != null ){
                            Map<String,String> currIdentityMap = new HashMap<String,String>();
                            currIdentityMap = currPd.getIdentity().getProperties();
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("hasIDProps"), Bytes.toBytes(true));
                            for (Map.Entry<String, String> entry : currIdentityMap.entrySet())
                            {
                                p.add(Bytes.toBytes(iot_secondary_colfam),
                                        Bytes.toBytes("IDProp__"+entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                    }
                    // events data
                    if (currPd.getEvents() != null){
                    	// increment events count and set up hbase cols according to count
                    	countEvents += 1;
                    	String curEventCntStr = "__" + Integer.toString(countEvents);
                    	// store the details in a map
                    	HashMap<String, String> currEventDataMap = new HashMap<String,String>();
                        p.add(Bytes.toBytes(iot_secondary_colfam),
                                Bytes.toBytes("hasEvents"), Bytes.toBytes(true));
                        if ( currPd.getEvents().getAction() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("EventsAction" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAction()));
                            currEventDataMap.put("EventsAction", currPd.getEvents().getAction());
                        }
                        if ( currPd.getEvents().getAppSessionId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("EventsAppSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getAppSessionId()));
                            currEventDataMap.put("EventsAppSessionID", currPd.getEvents().getAppSessionId());
                        }
                        if ( currPd.getEvents().getTimestamp() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("EventsTimeStamp" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getTimestamp()));
                            currEventDataMap.put("EventsTimeStamp", currPd.getEvents().getTimestamp());
                        }
                        if ( currPd.getEvents().getSessionId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("EventsSessionID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getSessionId()));
                            currEventDataMap.put("EventsSessionID", currPd.getEvents().getSessionId());
                        }
                        if ( currPd.getEvents().getUserId() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("EventsUserID" + curEventCntStr), Bytes.toBytes(currPd.getEvents().getUserId()));
                            currEventDataMap.put("EventsUserID", currPd.getEvents().getUserId());
                        }
                        if ( currPd.getEvents().getEvent() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("Events" + curEventCntStr),
                                    Bytes.toBytes(currPd.getEvents().getEvent()));
                            currEventDataMap.put("Event", currPd.getEvents().getEvent());
                        }
                        if ( currPd.getEvents().getPosition() != null ){
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("EventsPosition"), Bytes.toBytes(currPd.getEvents().getPosition()));
                            currEventDataMap.put("EventsPosition", Long.toString( currPd.getEvents().getPosition() ));
                        }
                        // get properties in a hashmap and put the data
                        if ( currPd.getEvents().getProperties() != null ){
                            Map<String,String> currEventsMap = new HashMap<String,String>();
                            currEventsMap = currPd.getEvents().getProperties();
                            p.add(Bytes.toBytes(iot_secondary_colfam),
                                    Bytes.toBytes("hasEventsProps"), Bytes.toBytes(true));
                            currEventDataMap.put("EventsProp", currPd.getEvents().getProperties().toString());
                            for (Map.Entry<String, String> entry : currEventsMap.entrySet())
                            {
                                p.add(Bytes.toBytes(iot_secondary_colfam),
                                        Bytes.toBytes("EventsProp"+ curEventCntStr + "__" + entry.getKey()), Bytes.toBytes(entry.getValue()));
                            }
                        }
                        finEventDataMap.put("Event" + curEventCntStr, currEventDataMap);
                    }
	        		// push action data
	        		if (currPd.getPUSHACTION() != null){
	        			p.add(Bytes.toBytes(iot_secondary_colfam),
								Bytes.toBytes("hasPushActn"), Bytes.toBytes(true));
	        			if ( currPd.getPUSHACTION().getAction() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("PushActnActionName"), Bytes.toBytes(currPd.getPUSHACTION().getAction()));
	        			}
	        			if ( currPd.getPUSHACTION().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("PushActnAppSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getPUSHKEY() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("PushActnPushKey"), Bytes.toBytes(currPd.getPUSHACTION().getPUSHKEY()));
	        			}
	        			if ( currPd.getPUSHACTION().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("PushActnTimestamp"), Bytes.toBytes(currPd.getPUSHACTION().getTimestamp()));
	        			}
	        			if ( currPd.getPUSHACTION().getSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("PushActnSessionID"), Bytes.toBytes(currPd.getPUSHACTION().getSessionId()));
	        			}
	        			if ( currPd.getPUSHACTION().getUserId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("PushActnUserID"), Bytes.toBytes(currPd.getPUSHACTION().getUserId()));
	        			}
	    			}
	        		// adv_id action data
	        		if (currPd.getADVERTISINGIDACTION() != null){
	        			p.add(Bytes.toBytes(iot_secondary_colfam),
								Bytes.toBytes("hasAdIDAction"), Bytes.toBytes(true));
	        			if ( currPd.getADVERTISINGIDACTION().getAction() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("AdIDActionActionName"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAction()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getAppSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("AdIDActionAppSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getAppSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("AdIDActionAdIDKey"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDKEY()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("AdIDActionTimestamp"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getTimestamp()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getSessionId() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("AdIDActionSessionID"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getSessionId()));
	        			}
	        			if ( currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("AdIDActionAdIDOptOut"), Bytes.toBytes(currPd.getADVERTISINGIDACTION().getADVERTISINGIDOPTOUT()));
	        			}
	    			}
	        		// new device data
	        		if (currPd.getNewDevice() != null){
	        			p.add(Bytes.toBytes(iot_secondary_colfam),
								Bytes.toBytes("hasNewDev"), Bytes.toBytes(true));
	        			if ( currPd.getNewDevice().getAction() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("NewDevAction"), Bytes.toBytes(currPd.getNewDevice().getAction()));
	        			}
	        			if ( currPd.getNewDevice().getTimestamp() != null ){
		        			p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("NewDevTimeStamp"), Bytes.toBytes(currPd.getNewDevice().getTimestamp()));
	        			}
	        			if ( currPd.getNewDevice().getContext() != null ){
	        				p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("hasNewDevCxt"), Bytes.toBytes(true));
	        				// context features data
	        				if ( currPd.getNewDevice().getContext().getFeatures() != null ){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasNewDevCxtFeatures"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasNFC() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("NewDevCxtFeaturesNFC"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasNFC()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasTelephony() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("NewDevCxtFeaturesTelephony"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasTelephony()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGPS() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGPS"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGPS()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesAcclroMtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasAccelerometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasBarometer() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBaromtr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasBarometer()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasCompass() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesCompass"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasCompass()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasGyroscope() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesGyro"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasGyroscope()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasLightsensor() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesLightSensr"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasLightsensor()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getHasProximity() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesProxmty"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getHasProximity()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("hasNewDevCxtFeaturesBTVrsn"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getFeatures().getBluetoothVersion()));
	        					}
	        					
	        				}
	        				// context display data
	        				if ( currPd.getNewDevice().getContext().getDisplay() != null ){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasNewDevCxtDisplay"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayHeight() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("NewDevCxtDisplayHeight"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayHeight()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayWidth() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("NewDevCxtDisplayWidth"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayWidth()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getDisplay().getDisplayDensity() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("NewDevCxtDisplayDensity"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getDisplay().getDisplayDensity()));
	        					}
	        				}
	        				// context total mem info data
	        				if ( currPd.getNewDevice().getContext().getTotalMemoryInfo() != null ){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasNewDevCxtTotalMemry"), Bytes.toBytes(true));
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryRAM"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalRAM()));
	        					}
	        					if ( currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("NewDevCxtTotalMemryStorage"), 
											Bytes.toBytes(currPd.getNewDevice().getContext().getTotalMemoryInfo().getTotalStorage()));
	        					}
	        				}
	        			}
	        			
	        		}
	        		// device info data
	        		if (currPd.getDeviceInfo() != null){
	        			p.add(Bytes.toBytes(iot_secondary_colfam),
								Bytes.toBytes("hasDevInfo"), Bytes.toBytes(true));
	        			if (currPd.getDeviceInfo().getAction() != null){
	        				p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("DevInfoAction"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAction()));
	        			}
	        			if (currPd.getDeviceInfo().getTimestamp() != null){
	        				p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("DevInfoTimestamp"), 
									Bytes.toBytes(currPd.getDeviceInfo().getTimestamp()));
	        			}
	        			if (currPd.getDeviceInfo().getAppSessionId() != null){
	        				p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("DevInfoAppSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getAppSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getSessionId() != null){
	        				p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("DevInfoSessionID"), 
									Bytes.toBytes(currPd.getDeviceInfo().getSessionId()));
	        			}
	        			if (currPd.getDeviceInfo().getContext() != null){
	        				p.add(Bytes.toBytes(iot_secondary_colfam),
									Bytes.toBytes("hasDevInfoCxt"), Bytes.toBytes(true));
	        				if (currPd.getDeviceInfo().getContext().getAppBuild() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtAppBuild"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getPackageName() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildPackageName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getPackageName()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnCode"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionCode()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getAppBuild().getVersionName() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtAppBuildVrsnName"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAppBuild().getVersionName()));
	        					}
	        				}
	        				// device info context device
	        				if (currPd.getDeviceInfo().getContext().getDevice() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtDevice"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getDevice().getSdkVersion() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtDeviceSDKVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getSdkVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtDeviceReleaseVrsn"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getReleaseVersion()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBrand"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBrand()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtDeviceManfactrer"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceManufacturer()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceModel() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtDeviceModel"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceModel()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtDeviceBoard"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceBoard()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtDeviceProduct"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getDevice().getDeviceProduct()));
	        					}
	        				}
	        				// device info context locale
	        				if (currPd.getDeviceInfo().getContext().getLocale() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtLocale"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevCountry"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceCountry()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtLocaleDevLang"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocale().getDeviceLanguage()));
	        					}
	        				}
	        				// device info context location
	        				if (currPd.getDeviceInfo().getContext().getLocation() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtLocation"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLatitude() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtLocationLat"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLatitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getLongitude() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtLocationLong"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getLongitude()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getLocation().getSpeed() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtLocationSpeed"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getLocation().getSpeed()));
	        					}
	        				}
	        				// device info context telephone
	        				if (currPd.getDeviceInfo().getContext().getTelephone() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtTelephone"), Bytes.toBytes(true));
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnCarrier"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneCarrier()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtTelephonePhnRadio"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getPhoneRadio()));
	        					}
	        					if (currPd.getDeviceInfo().getContext().getTelephone().getInRoaming() != null){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtTelephoneInRoaming"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getTelephone().getInRoaming()));
	        					}
	        				}
	        				// device info context wifi
	        				if (currPd.getDeviceInfo().getContext().getWifi() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtWifi"), Bytes.toBytes(true));
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("DevInfoCxtWifi"), 
										Bytes.toBytes(currPd.getDeviceInfo().getContext().getWifi().toString()));
	        				}
	        				// device info context bluetoothInfo
	        				if (currPd.getDeviceInfo().getContext().getBluetoothInfo() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtBTInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus() != null ){
		        					p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtBTInfoBTStatus"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getBluetoothInfo().getBluetoothStatus()));
	        					}
	        				}
	        				// device info context availableMemoryInfo
	        				if (currPd.getDeviceInfo().getContext().getAvailableMemoryInfo() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtAvailbleMemryInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM() != null ){
		        					p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailRAM"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableRAM()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage() != null ){
		        					p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtAvailbleMemryInfoAvailStorage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getAvailableMemoryInfo().getAvailableStorage()));
	        					}
	        				}
	        				// device info context cpuInfo
	        				if (currPd.getDeviceInfo().getContext().getCpuInfo() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtCPUInfo"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal() != null ){
		        					p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoTotal"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuTotal()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle() != null ){
		        					p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoIdle"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuIdle()));
	        					}
	        					if ( currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage() != null ){
		        					p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtCPUInfoUsage"), 
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getCpuInfo().getCpuUsage()));
	        					}
	        				}
	        				// device info context USER_AGENT_ACTION
	        				if (currPd.getDeviceInfo().getContext().getUSERAGENTACTION() != null){
	        					p.add(Bytes.toBytes(iot_secondary_colfam),
										Bytes.toBytes("hasDevInfoCxtUsrAgntActn"), Bytes.toBytes(true));
	        					if ( currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent() != null ){
	        						p.add(Bytes.toBytes(iot_secondary_colfam),
											Bytes.toBytes("DevInfoCxtUsrAgntActnUsrAgnt"),
											Bytes.toBytes(currPd.getDeviceInfo().getContext().getUSERAGENTACTION().getUserAgent()));
	        					}
	        				}
	        			}
	        		}
	        	}
	        	
	        	p.add(Bytes.toBytes(iot_secondary_colfam),
						Bytes.toBytes("eventDataMap"), Bytes.toBytes(finEventDataMap.toString()));
	        	
	        	
	        	secondaryTable.put(p);
	        	
	}
	
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtPushDevDatatoPut(String deviceId, String appSecret, String PushKey) {				
		// put the data
		String currRowKey = deviceId;
		System.out.println("^^^^^^^^^^^^^^^^^^^^^ push dev data: " + currRowKey + " -- " + appSecret + "__PUSH" + " -- " + PushKey);
		Put pushDevIdPut = new Put(Bytes.toBytes(currRowKey));
		
		// add basic data to corresponding columns
		pushDevIdPut.add(Bytes.toBytes(push_adid_dev_map_tab_cf), Bytes.toBytes(appSecret + "__PUSH"), Bytes.toBytes(PushKey));
				
		// return the put object
	    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), pushDevIdPut);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtEventDBEventDatatoPut(String eventPrimary, String eventSecondary, String appSecret,
																			String date, long countEvents) {	
		// put the data
		String currRowKey = appSecret + "__" + eventPrimary + "__" + date;
		Put eventDBPut = new Put(Bytes.toBytes(currRowKey));
	
		// add basic data to corresponding columns
		eventDBPut.add(Bytes.toBytes(eventDB_event_count_tab_cf), Bytes.toBytes(eventSecondary), Bytes.toBytes(countEvents));
				
		// return the put object
	    return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), eventDBPut);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtAdidDevDatatoPut(String deviceId, String appSecret, String adid, String idType) {
		// put the data
		String currRowKey = deviceId;
		Put adIdPut = new Put(Bytes.toBytes(currRowKey));
		
		// add basic data to corresponding columns
		adIdPut.add(Bytes.toBytes(push_adid_dev_map_tab_cf), Bytes.toBytes(appSecret + "__" + idType), Bytes.toBytes(adid));
				
		// return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), adIdPut);
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> cnvrtPushDatatoPut(String deviceId, String appSecret, String PushKey) {
		// put the data
		String currRowKey = appSecret + "__" + deviceId;
		System.out.println("^^^^^^^^^^^^^^^ push data to put: " + currRowKey + " -- " + PushKey);
		Put pushDataPut = new Put(Bytes.toBytes(currRowKey));
			
		// add basic data to corresponding columns
		pushDataPut.add(Bytes.toBytes(push_appsecret_dev_tab_cf), Bytes.toBytes("push_key"), Bytes.toBytes(PushKey));
					
		// return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), pushDataPut);
		
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> activeUsersPreprocess(String rowkey, String appsecret, String deviceId, long timestamp, String date) {
		//put the data
		System.out.println("^^^^^^^^^^^^^^^^^^ active users preprocess put: "+rowkey+"--"+appsecret+"--"+deviceId+"--"+timestamp+"--"+date);
		Put activePreprocessPut = new Put(Bytes.toBytes(rowkey));
			
		//add active users preprocess data to hbase
		activePreprocessPut.add(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		activePreprocessPut.add(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("device_id"), Bytes.toBytes(deviceId));
		activePreprocessPut.add(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
		activePreprocessPut.add(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("date"), Bytes.toBytes(date));
		/*if(deviceModel != null) {
			System.out.println("+++++++++++++++++++++++++++++++++++++++= INSIDE DEVICE MODEL WALA CONDITION ++++++++++++++++++++++++++++++++++++++++");
			p.add(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("device_model"), Bytes.toBytes(deviceModel));
			System.out.println("+++++++++++++++++++++++++++++++++++++++= INSIDE DEVICE MODEL WALA CONDITION ++++++++++++++++++++++++++++++++++++++++");
		}*/

		//return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), activePreprocessPut);
	
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> newUsersPreprocess(String rowkey, String appsecret, String deviceId, long timestamp, String date) {
		//put the data
		System.out.println("^^^^^^^^^^^^^^^^^^ new users preprocess put: "+rowkey+"--"+appsecret+"--"+deviceId+"--"+timestamp+"--"+date);
		Put newPreprocessPut = new Put(Bytes.toBytes(rowkey));
	
		//add new users preprocess data to hbase
		newPreprocessPut.add(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		newPreprocessPut.add(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("device_id"), Bytes.toBytes(deviceId));
		newPreprocessPut.add(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("timestamp"), Bytes.toBytes(timestamp));
		newPreprocessPut.add(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("date"), Bytes.toBytes(date));
				
		//return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), newPreprocessPut);
		
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> getActiveUsers(String rowKey, String appSecret, long activeUsers) {
		//put the data
		System.out.println("^^^^^^^^^^^^^^^^^^ ACTIVE users put: "+"--"+rowKey+"--"+appSecret+"--"+activeUsers);
		Put AUPut = new Put(Bytes.toBytes(rowKey));
			
		//add active users data to hbase
		AUPut.add(Bytes.toBytes(active_users_tab_cf), Bytes.toBytes("app_secret"), Bytes.toBytes(appSecret));
		AUPut.add(Bytes.toBytes(active_users_tab_cf), Bytes.toBytes("active_users"), Bytes.toBytes(activeUsers));
				
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), AUPut);
		
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> getNewUsers(String rowKey, String appSecret, long newUsers) {
		//put the data
		System.out.println("^^^^^^^^^^^^^^^^^^ ACTIVE users put: "+"--"+rowKey+"--"+appSecret+"--"+newUsers);	
		Put NUPut = new Put(Bytes.toBytes(rowKey));
			
		//add new users data to hbase
		NUPut.add(Bytes.toBytes(new_users_tab_cf), Bytes.toBytes("app_secret"), Bytes.toBytes(appSecret));
		NUPut.add(Bytes.toBytes(new_users_tab_cf), Bytes.toBytes("new_users"), Bytes.toBytes(newUsers));
				
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), NUPut);
		
	}
	
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> getCumulativeEventsCount(String rowkey, String appsecret, String eventname, Long eventcount) {
		//put the data
		System.out.println("^^^^^^^^^^^^^^^^^^ cumulative events count put: "+rowkey+"--"+appsecret+"--"+eventname+"--"+eventcount);
		Put eventsPut = new Put(Bytes.toBytes(rowkey));
			
		//add data for cumulative event counts to hbase
		eventsPut.add(Bytes.toBytes(hbaseTableEvents2_cf), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		if(eventname != null) {
			eventsPut.add(Bytes.toBytes(hbaseTableEvents2_cf), Bytes.toBytes(eventname), Bytes.toBytes(eventcount));
		}
				
		//return object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), eventsPut);
		
	}
	
	//event wise active users data dump to hbase
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> doEventwiseSegmentationAU(String rowkey, String appsecret, String eventname, long userCount) {
		//put the data
		System.out.println("^^^^^^^^^^^^^^^^^^^^ event-wise AU segmentation: "+rowkey+"--"+appsecret+"--"+eventname+"--"+userCount);
		Put eventsAUPut = new Put(Bytes.toBytes(rowkey));
			
		eventsAUPut.add(Bytes.toBytes(hbaseTableEventsAU_cf), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		if(eventname != null) {
			logger.info("AU eventwise segmentation with eventname ---> " + eventname + " and eventcount as ---> " + userCount);
			eventsAUPut.add(Bytes.toBytes(hbaseTableEventsAU_cf), Bytes.toBytes(eventname), Bytes.toBytes(userCount));
		}
					
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), eventsAUPut);
	
	}
	
	//event wise new users data dump to hbase
	@SuppressWarnings("deprecation")
	public Tuple2<ImmutableBytesWritable, Put> doEventwiseSegmentationNU(String rowkey, String appsecret, String eventname, long userCount) {
		//put the data
		System.out.println("^^^^^^^^^^^^^^^^^^^^ event-wise NU segmentation: "+rowkey+"--"+appsecret+"--"+eventname+"--"+userCount);
		Put eventsNUPut = new Put(Bytes.toBytes(rowkey));
			
		eventsNUPut.add(Bytes.toBytes(hbaseTableEventsNU_cf), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		if(eventname != null) {
			logger.info("AU eventwise segmentation with eventname ---> " + eventname + " and eventcount as ---> " + userCount);
			eventsNUPut.add(Bytes.toBytes(hbaseTableEventsNU_cf), Bytes.toBytes(eventname), Bytes.toBytes(userCount));
		}
						
		//return put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), eventsNUPut);
		
	}
	
	public JavaRDD<ESDataBean> prepareForElasticSearch() {
		hbaseIotConf.set(TableInputFormat.INPUT_TABLE, AUpreprocess);
		hbaseIotConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, AUpreprocess_cf); // column family
		String hbaseReadColList = AUpreprocess_cf + ":rowKey " + AUpreprocess_cf + ":app_secret " + AUpreprocess_cf + ":device_id " +
								  AUpreprocess_cf + ":timestamp " + AUpreprocess_cf + ":date " + AUpreprocess_cf + ":year " + AUpreprocess_cf + ":month " +
								  AUpreprocess_cf + ":day ";
		hbaseIotConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> AUPrePairRDD = TheInThings2.jsc.newAPIHadoopRDD(hbaseIotConf, TableInputFormat.class, 
				 															ImmutableBytesWritable.class, Result.class);
		
		JavaRDD<ESDataBean> AUPreES = AUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, ESDataBean>() {
			public ESDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowkey = Bytes.toString(r.getRow());
				esbean = new ESDataBean();
				esbean.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("app_secret"))));
				esbean.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("device_id"))));
				esbean.setDate(Bytes.toString(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("timestamp"))));
				esbean.setDate(Bytes.toString(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("date"))));
				
				return esbean;
			}
		});
		
		return AUPreES;
	}
	
	public Dataset<Row> getActiveUsersPreprocessHbase(JavaSparkContext jsc, SparkSession spark) {
		hbaseIotConf.set(TableInputFormat.INPUT_TABLE, AUpreprocess);
		hbaseIotConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, AUpreprocess_cf); // column family
		String hbaseReadColList = AUpreprocess_cf + ":rowKey " + 
								  AUpreprocess_cf + ":app_secret " + 
								  AUpreprocess_cf + ":device_id " +
								  AUpreprocess_cf + ":timestamp " + 
								  AUpreprocess_cf + ":date ";
	
		hbaseIotConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> AUPrePairRDD = jsc.newAPIHadoopRDD(hbaseIotConf, TableInputFormat.class, 
				 															ImmutableBytesWritable.class, Result.class);
		JavaRDD<IotRawDataBean> AUPreprocessRDD = AUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, IotRawDataBean>() {
			public IotRawDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowkey = Bytes.toString(r.getRow());
				iot = new IotRawDataBean();
				iot.setiotRawDataRowKey(rowkey);
				iot.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("app_secret"))));
				iot.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("device_id"))));
				iot.settimeStamp(Bytes.toLong(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("timestamp"))));
				iot.setDate(Bytes.toString(r.getValue(Bytes.toBytes(AUpreprocess_cf), Bytes.toBytes("date"))));
				
				return iot;
			}
		});
		
		Dataset<Row> AUPreprocessDS = spark.createDataFrame(AUPreprocessRDD, IotRawDataBean.class);
		return AUPreprocessDS;
	}
	
	public Dataset<Row> getNewUsersPreprocessHbase(JavaSparkContext jsc, SparkSession spark) {
		hbaseIotConf.set(TableInputFormat.INPUT_TABLE, NUpreprocess);
		hbaseIotConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, NUpreprocess_cf); // column family
		String hbaseReadColList = NUpreprocess_cf + ":rowKey " +
								NUpreprocess_cf + ":app_secret " +
								NUpreprocess_cf + ":device_id " +
								NUpreprocess_cf + ":timestamp " +
								NUpreprocess_cf + ":date ";
		
		hbaseIotConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> NUPrePairRDD = jsc.newAPIHadoopRDD(hbaseIotConf, TableInputFormat.class, 
						 															ImmutableBytesWritable.class, Result.class);
		JavaRDD<IotRawDataBean> NUPreprocessRDD = NUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, IotRawDataBean>() {
			public IotRawDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowkey = Bytes.toString(r.getRow());
				iot = new IotRawDataBean();
				iot.setiotRawDataRowKey(rowkey);
				iot.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("app_secret"))));
				iot.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("device_id"))));
				iot.settimeStamp(Bytes.toLong(r.getValue(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("timestamp"))));
				iot.setDate(Bytes.toString(r.getValue(Bytes.toBytes(NUpreprocess_cf), Bytes.toBytes("date"))));
				
				return iot;
			}
		});
		
		Dataset<Row> NUPreprocessDS = spark.createDataFrame(NUPreprocessRDD, IotRawDataBean.class);
		return NUPreprocessDS;
	}
	
	public Dataset<Row> getEvents1HbaseTable(JavaSparkContext jsc, SparkSession spark, Configuration hbaseConf) {
		hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableEvents1);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseTableEvents1_cf);	//column family
		String hbaseReadColList = hbaseTableEvents1_cf + ":rowKey " + hbaseTableEvents1_cf + ":app_secret " + hbaseTableEvents1_cf + ":device_id " +
								  hbaseTableEvents1_cf + ":server_time " + hbaseTableEvents1_cf + ":event_name " + hbaseTableEvents1_cf + ":event_properties ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList);
		
		JavaPairRDD<ImmutableBytesWritable, Result> events1PairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
																										ImmutableBytesWritable.class, Result.class);
		JavaRDD<IotRawDataBean> events1RDD = events1PairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, IotRawDataBean>() {
			public IotRawDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowKey = Bytes.toString(r.getRow());
				iot = new IotRawDataBean();
				iot.setiotRawDataRowKey(rowKey);
				iot.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_cf), Bytes.toBytes("app_secret"))));
				iot.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_cf), Bytes.toBytes("device_id"))));
				iot.setiotRawDataServerTime(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_cf), Bytes.toBytes("server_time"))));
				iot.setiotRawDataEventName(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_cf), Bytes.toBytes("event_name"))));
				iot.setiotRawDataEventProperties(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_cf), Bytes.toBytes("event_properties"))));
				return iot;
			}
		});
		
		Dataset<Row> events1DS = spark.createDataFrame(events1RDD, IotRawDataBean.class);
		return events1DS;
	}
	
}