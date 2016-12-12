package com.iot.data.queries;

import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.StringUtils;

import scala.Tuple2;

public class EventsCount {
	private static String confTypeFile, confFileName;
	private static JSONParser confParser;
	private static Object confObj;
	private static JSONObject confJsonObject;
	
	private static String iot_events1_tab_name;
	private static String iot_events1_colfamily_name;
	private static String event_count_tab_name;
	private static String event_count_tab_colfam;
	
	private static Job newAPIJobConfigEventsDBEvents;
	
	private final static Logger logger = LoggerFactory.getLogger(EventsCount.class);
	
	static {
		try {
			//conf file settings
			confTypeFile = "production_conf.json";
			confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
			confParser = new JSONParser();
			confObj = confParser.parse(new FileReader(confFileName));
			confJsonObject = (JSONObject) confObj;
			
			iot_events1_tab_name = (String) confJsonObject.get("hbase_table_events1");
			iot_events1_colfamily_name = (String) confJsonObject.get("hbase_table_events1_colfam");
			event_count_tab_name = (String) confJsonObject.get("hbase_events_count");
			event_count_tab_colfam = (String) confJsonObject.get("hbase_events_count_colfam");
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(ParseException e) {
			e.printStackTrace();
		}
		
	}

	public static void getEventsCountData(Configuration hbaseConf, JavaSparkContext jsc) throws Exception {
		// settings for writing to event count table
		newAPIJobConfigEventsDBEvents = Job.getInstance(hbaseConf);
		newAPIJobConfigEventsDBEvents.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, event_count_tab_name);
		newAPIJobConfigEventsDBEvents.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		
		// set table and column names
		hbaseConf.set(TableInputFormat.INPUT_TABLE, iot_events1_tab_name);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, iot_events1_colfamily_name); // column family 
		String hbaseReadColList = iot_events1_colfamily_name+":app_secret " +
				iot_events1_colfamily_name+":packet_id " +
				iot_events1_colfamily_name+":event_properties " +
				iot_events1_colfamily_name+":device_id " +
				iot_events1_colfamily_name+":server_time " +
				iot_events1_colfamily_name+":eventName ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers

		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = 
				jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		// filter out all null valued data
		// properly format the data into a new RDD 
		JavaRDD<Row> eventDatRDD = hBaseRDD.flatMap(
				new FlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, Row>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 5876715536325624747L;

					public Iterator<Row> call( Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
						// get the rowid and set it as key in the pairRDD
						ArrayList<Row> output = new ArrayList<Row>();
						Result r = entry._2;
						String keyRow = Bytes.toString(r.getRow());
						String iotRawDataRowKey=keyRow;
						String iotRawDataPacketId="";
						iotRawDataPacketId = (String) Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("packet_id")));
						String iotRawDataAppSecret= "" ;
						iotRawDataAppSecret = (String) Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("app_secret")));
						String iotRawDataDeviceId= "";
						iotRawDataDeviceId = (String) Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("device_id")));
						String iotRawDataServerTime="";
						iotRawDataServerTime = (String) Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("server_time")));
						Long timestamp =0L;
						timestamp = Long.parseLong(Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("server_time"))));
						String otherparameters = 	Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("event_properties")));
						org.codehaus.jettison.json.JSONObject obj = new org.codehaus.jettison.json.JSONObject(otherparameters);
						String eventName = "";
						eventName = (String) Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("event_name")));
						Date date = new Date(Long.parseLong(Bytes.toString(r.getValue(Bytes.toBytes(iot_events1_colfamily_name), Bytes.toBytes("server_time"))))*1000L);
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
						String dateStr= ""; 
						dateStr = sdf.format(date);
						Iterator <String> keys = obj.keys();
						if((!StringUtils.isEmptyOrWhitespaceOnly(iotRawDataAppSecret))&&(!StringUtils.isEmptyOrWhitespaceOnly(eventName))){
							while(keys.hasNext()){
								String param = keys.next();
								if (param!=null?param.length()>0:false){
									String value="!@#$%^&*()";
									if(obj.get(param)!=null&&!StringUtils.isEmptyOrWhitespaceOnly(obj.get(param).toString()))
										value= obj.get(param).toString();
									output.add(RowFactory.create(dateStr, iotRawDataAppSecret,iotRawDataDeviceId, timestamp,eventName,param, value));
								}
							}
						}
						return output.iterator();
					}
				});
		System.out.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n"+eventDatRDD.count() +"\n" + eventDatRDD.first().toString()+"\n\n\n\n\n\n\n\n\n\n\n\n\n\n");

		JavaPairRDD<String ,Long> eventCountRDD = eventDatRDD.mapToPair(new PairFunction<Row, String, Long >() {

			public Tuple2<String, Long> call(Row r) throws Exception {
				// TODO Auto-generated method stub
				String key ="";
				//key contains iotRawDataAppSecret,dateStr,eventName,param, value
				key = r.getString(1) +"__"+ r.getString(0) +"__"+ r.getString(4) +"__"+ r.getString(5) +"__"+ r.getString(6);
				return new Tuple2<String, Long>(key,1L);
			}

		}).reduceByKey(new Function2<Long, Long, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -4143377124291280961L;

			public Long call(Long a, Long b) throws Exception {
				// TODO Auto-generated method stub
				return (Long)(a+b);
			}
		});
	    JavaPairRDD<ImmutableBytesWritable, Put> eventsDBEventsCountToPut = eventCountRDD.mapToPair(new PairFunction<Tuple2<String,Long>,ImmutableBytesWritable, Put >() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -9197743060540111206L;

			@SuppressWarnings("deprecation")
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Long> t) throws Exception {
				// TODO Auto-generated method stub
				String parts[] =t._1.split("__");
				logger.info("Data to be entered ===> : " + parts[0]+"__"+parts[1]+"__"+parts[2]+"__"+parts[3]+"__"+parts[4]+" Count: "+t._2.toString());
				// put the data ( new key is iotRawDataAppSecret,dateStr,eventName,param)
				String currRowKey =parts[0]+"__"+parts[1]+"__"+parts[2]+"__"+parts[3];
				Put p = new Put(Bytes.toBytes(currRowKey));
				// add basic data to corresponding columns ( column name : value of param , column value : count )
				p.add(Bytes.toBytes(event_count_tab_colfam),
						Bytes.toBytes(parts[4]), Bytes.toBytes(t._2));
				// return the put object
				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(currRowKey)), p);
			// if something fails!!
			}
		});
	    eventsDBEventsCountToPut.saveAsNewAPIHadoopDataset(newAPIJobConfigEventsDBEvents.getConfiguration());
	    jsc.close();;
		
	}
	
}
