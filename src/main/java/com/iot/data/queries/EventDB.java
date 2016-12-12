package com.iot.data.queries;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import com.iot.data.databeans.EventDataBean;
import com.iot.data.utils.HbaseUtils;

public class EventDB implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// conf file settings
	private static String confFileName;
	private static String confTypeFile;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	
	//private static String raw_iot_tab_colfam;
	private static Job newAPIJobConfigEventsDBEvents;
	private static HbaseUtils hbaseUtilsObj;
	
	// hbase tabs 
	private static String hbaseTableEvents1;
	private static String hbaseTableEvents1_colfam;
	private static String eventDB_event_count_tab_name;
	private static String eventDB_event_count_tab_colfam;
	
	//misc variables
	private static EventDataBean evntDat ; 
	
	private final static Logger logger = LoggerFactory.getLogger(EventDB.class);
	
	public EventDB() {
		try {
			// settings for production or testing (choose one)
			confTypeFile = "production_conf.json";
			// read conf file and corresponding params
			confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
			// read the json file and create a map of the parameters
			confObj = confParser.parse(new FileReader(confFileName));
	        confJsonObject = (JSONObject) confObj;
	        
	 	    hbaseUtilsObj = new HbaseUtils(confTypeFile);
	 	   
	 	    // hbase table conf
	        hbaseTableEvents1 = (String) confJsonObject.get("hbase_table_events1");
	        hbaseTableEvents1_colfam = (String) confJsonObject.get("hbase_table_events1_colfam");
	        
	        eventDB_event_count_tab_name = (String) confJsonObject.get("hbase_eventsDBevents");
	        eventDB_event_count_tab_colfam = (String) confJsonObject.get("hbase_eventsDBevents_colfam");
	 	    
	        // settings for writing to push adid table
	        newAPIJobConfigEventsDBEvents = Job.getInstance(TheInThings.hbaseConf);
	        newAPIJobConfigEventsDBEvents.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, eventDB_event_count_tab_name);
	        newAPIJobConfigEventsDBEvents.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(ParseException p) {
			p.printStackTrace();
		}
	}
	
	public void getEventData(Configuration hbaseConf, JavaSparkContext jsc, SparkSession spark) throws Exception {
	    // set table and column names
	    hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableEvents1);
	    hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseTableEvents1_colfam); // column family 
	    String hbaseReadColList = hbaseTableEvents1_colfam + ":app_secret " +
	    						hbaseTableEvents1_colfam + ":packet_id " +
	    						hbaseTableEvents1_colfam + ":device_id " +
	    						hbaseTableEvents1_colfam + ":server_time " +
	    						hbaseTableEvents1_colfam + ":event_name ";
	    hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
	    
	    // read data into rdd
	    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
	    
	    
	    // filter out all null valued data
	    // properly format the data into a new RDD
	    JavaRDD<EventDataBean> eventDatRDD = hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, EventDataBean>() {
	         	public EventDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
	            	// get the rowid and set it as key in the pairRDD
	                Result r = entry._2;
	                String keyRow = Bytes.toString(r.getRow());
	             
	                // define java bean  
	                evntDat = new EventDataBean();
	                // set values from hbase
	                evntDat.setiotRawDataRowKey(keyRow);
	                evntDat.setiotRawDataAppSecret((String) Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("app_secret"))));
	                evntDat.setiotRawDataPacketId((String) Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("packet_id"))));
	                evntDat.setiotRawDataDeviceId((String) Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("device_id"))));
	                evntDat.setiotRawDataServerTime((String) Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("server_time"))));	                
	                // set timestamp
	                evntDat.settimeStamp(Long.parseLong(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("server_time")))));
	                // set event details
	                evntDat.seteventName((String) Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("event_name"))));
	                
	            return evntDat;
	        }
	    });
	   
	    Dataset<Row> eventDS = spark.createDataFrame(eventDatRDD, EventDataBean.class);
	    
	    if(eventDS.count() > 0) {
	    	// make it a table to execute sql queries
		    eventDS.createOrReplaceTempView("eventTab");		    

		    Dataset<Row> eventDBEventsDS = TheInThings.spark.sql("SELECT e1.eventName, e2.eventName, e1.iotRawDataAppSecret, "
		    							+ " CONCAT(from_unixtime(e1.timeStamp, 'Y'), '__', from_unixtime(e1.timeStamp, 'M'), '__', "
		    							+ " from_unixtime(e1.timeStamp, 'd')) AS date, COUNT(DISTINCT(e1.iotRawDataDeviceId)) AS eventCount "
		    							+ " FROM eventTab AS e1 INNER JOIN "
		    							+ " (SELECT eventName, iotRawDataDeviceId, iotRawDataAppSecret, dateStr FROM eventTab) AS e2 "
		    							+ " ON (e1.iotRawDataDeviceId = e2.iotRawDataDeviceId) AND "
		    							+ " (e1.iotRawDataAppSecret = e2.iotRawDataAppSecret) AND (e1.eventName != e2.eventName) "
		    							+ " WHERE "
		    							+ " e1.iotRawDataDeviceId IS NOT NULL AND e1.eventName IS NOT NULL AND e2.eventName IS NOT NULL AND "
		    							+ " CONCAT(from_unixtime(e1.timeStamp, 'Y'), '__', from_unixtime(e1.timeStamp, 'M'), '__', "
		    							+ " from_unixtime(e1.timeStamp, 'd')) IS NOT NULL "
		    							+ " GROUP BY e1.eventName, e2.eventName, e1.iotRawDataAppSecret, "
		    							+ " CONCAT(from_unixtime(e1.timeStamp, 'Y'), '__', from_unixtime(e1.timeStamp, 'M'), '__', "
		    							+ " from_unixtime(e1.timeStamp, 'd'))");
	    	
		    // convert to RDD and put data into hbase
		    JavaRDD<Row> eventsDBEventsRDD = eventDBEventsDS.javaRDD();
		    JavaPairRDD<ImmutableBytesWritable, Put> eventsDBEventToPut = eventsDBEventsRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row evdbevRowObj) throws Exception {
					logger.info("evdbevRowObj to hbase---> " + evdbevRowObj.getString(0) + "__" + evdbevRowObj.getString(1) + "__" + 
									evdbevRowObj.getString(2) + "__" + evdbevRowObj.getString(3)  + "__" + evdbevRowObj.getLong(4));
					return hbaseUtilsObj.cnvrtEventDBEventDatatoPut(evdbevRowObj.getString(0), evdbevRowObj.getString(1), evdbevRowObj.getString(2), 
																evdbevRowObj.getString(3), evdbevRowObj.getLong(4)); 
				}
			});
		    
		    if(eventsDBEventToPut.count() > 0) {
		    	eventsDBEventToPut.saveAsNewAPIHadoopDataset(newAPIJobConfigEventsDBEvents.getConfiguration());
		    }
	    }
	}
	
}