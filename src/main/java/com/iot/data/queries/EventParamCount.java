package com.iot.data.queries;

import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class EventParamCount {
	
	private static String confTypeFile;
	private static String confFileName;
	private static JSONParser confParser;
	private static Object confObj;
	private static JSONObject confJsonObject;
	private static String eventscount_tab_name;
	private static String eventscount_colfamily_name;
	private static String eventparam_count_tab_colfam;
	private static String eventparam_count_tab_name;
	private static Job newAPIJobConfigEventsDBEvents;
	
	private static Logger logger = LoggerFactory.getLogger(EventParamCount.class);
	
	static {
		try {
			//conf file settings
			confTypeFile = "production_conf.json";
			confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
			confParser = new JSONParser();
			confObj = confParser.parse(new FileReader(confFileName));
			confJsonObject = (JSONObject) confObj;
			
			eventscount_tab_name = (String) confJsonObject.get("hbase_events_count");
			eventscount_colfamily_name = (String) confJsonObject.get("hbase_events_count_colfam");
			eventparam_count_tab_name = (String) confJsonObject.get("hbase_eventparam_count");
			eventparam_count_tab_colfam = (String) confJsonObject.get("hbase_eventparam_count_colfam");
			
		} catch(ParseException e) {
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	public static void getEventParamCount(Configuration hbaseConf, JavaSparkContext jsc) throws Exception{
		hbaseConf.set(TableInputFormat.INPUT_TABLE, eventscount_tab_name);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, eventscount_colfamily_name); // column family 
		
		// settings for writing to event param count table
		newAPIJobConfigEventsDBEvents = Job.getInstance(hbaseConf);
		newAPIJobConfigEventsDBEvents.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, eventparam_count_tab_name);
		newAPIJobConfigEventsDBEvents.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		
		// read data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = 
				jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		
		JavaPairRDD<String,String> eventDatRDD = hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, String >() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -7371517740092158103L;

			public String call(Tuple2<ImmutableBytesWritable, Result> t)
					throws Exception {
				Result r = t._2;
				String keyRow = Bytes.toString(r.getRow());
				return (keyRow);
			}
		}).distinct().mapToPair(new PairFunction<String, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 4497000168161924967L;

			public Tuple2<String, String> call(String t) throws Exception {
				String parts[] = t.split("__");
				String newKey = "";
				newKey = parts[0] + "__" + parts[1] + "__" + parts[2] ;
				return new Tuple2<String, String>(newKey, parts[3]);
			}
		});
		
		JavaPairRDD<ImmutableBytesWritable, Put> eventsDBEventsCountToPut = eventDatRDD.mapToPair(new PairFunction<Tuple2<String,String>,ImmutableBytesWritable, Put >() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -9197743060540111206L;

			@SuppressWarnings("deprecation")
			public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, String> t) throws Exception {
				// iotRawDataAppSecret,dateStr,eventName
				logger.info("Data to be entered ===> : " + t._1  +" Count: "+t._2);
				// put the data ( new key is iotRawDataAppSecret,dateStr)
				Put p = new Put(Bytes.toBytes(t._1));
				// add basic data to corresponding columns ( column name : eventname , column value : count )
				p.add(Bytes.toBytes(eventparam_count_tab_colfam),
						Bytes.toBytes(t._2), Bytes.toBytes(1));
				// return the put object
				return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(t._1)), p);
			// if something fails!!
			}
		});
		
		 eventsDBEventsCountToPut.saveAsNewAPIHadoopDataset(newAPIJobConfigEventsDBEvents.getConfiguration());
		
	}
	
}
