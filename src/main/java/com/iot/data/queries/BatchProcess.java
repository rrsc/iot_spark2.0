package com.iot.data.queries;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iot.data.databeans.ESDataBean;
import com.iot.data.databeans.IotRawDataBean;
import com.iot.data.processes.HbaseTableEvents;
import com.iot.data.schema.RcvdData;
import com.iot.data.schema.SerializableIotData;
import com.iot.data.utils.HbaseUtils;

import scala.Tuple2;

public class BatchProcess implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static BatchProcess inthings;
	
	//misc variables
	private static String confTypeFile, confFileName;
	private static JSONParser confParser;
	private static Object confObj;
	private static JSONObject confJsonObject;
	private static Properties props;
	private static IotRawDataBean iot;
	
	//hadoop and spark variables
	public static Configuration hbaseConf;
	private static String hbase_master_ip, hbase_master_port, hbase_zookeeper_port;
	public static JavaSparkContext jsc;
	private static SparkConf sparkConf;
	public static SparkSession spark;

	private static Job pushKeyAdIdJob;
	private static Job pushDevAppsecJob;
	private static Job activeUsersPreprocessJob;
	private static Job activeUsersJob;
	private static Job newUsersPreprocessJob;
	private static Job newUsersJob;
	private static Job newAPIJobConfigEventsDBEvents;
	
	private ESDataBean esbean;
	
	//hbase variables
	public static HbaseUtils hbaseUtilsObj;
	private static String iot_secondary, iot_secondary_cf, dev_map_apps, push_appsecret_dev;
	private static String activeUsersPreprocess, activeUsers, newUsersPreprocess, newUsers;
	private static String activeUsersPreprocess_cf;
	private static String eventCount, eventsAU, eventsNU;
	private static String event_count_tab_name;
	private static HTable secondaryTable;
	private static List<Delete> deletionRowKeys;
	
	//kafka variables
	private static String raw_iot_server_ip, raw_iot_spark_kafka_port, zk_node_conn_port, raw_iot_zk_conn_timeout_milsec;
	private static String kafka_consumer_id_secondary, kafka_consumer_group_id_secondary, iot_spark_driver_memory, iot_streaming_block_size;
	
	//avro variables
	//private static Decoder iotDecoder;
	private static DatumReader<RcvdData> iotRawDatumReader;
	private static SerializableIotData iotDataPacket;
	
	private final static Logger logger = LoggerFactory.getLogger(BatchProcess.class);
	
	public BatchProcess() {
		try {
			//conf file settings
			confTypeFile = "production_conf.json";
			confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
			confParser = new JSONParser();
			confObj = confParser.parse(new FileReader(confFileName));
	        confJsonObject = (JSONObject) confObj;
	        hbase_master_ip = (String) confJsonObject.get("server_ip");
	        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
	        hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
	        
	        // conf for reading from/ writing to hbase
	  		hbaseConf = HBaseConfiguration.create();
	 	    hbaseConf.set("hbase.master",hbase_master_ip + ":" + hbase_master_port);
	 	    hbaseConf.set("hbase.zookeeper.quorum", hbase_master_ip);
	 	    hbaseConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);
	 	    
	 	    hbaseUtilsObj = new HbaseUtils(confTypeFile);
	 	    
	 	    iot_secondary = (String) confJsonObject.get("hbase_table_secondary");
	 	    iot_secondary_cf = (String) confJsonObject.get("hbase_table_secondary_colfam");
	 	    
	 	    //secondaryTable = new HTable(hbaseConf, iot_secondary);
	 	    
	 	    dev_map_apps = (String) confJsonObject.get("hbase_table_dev_map_apps");
	 	    push_appsecret_dev = (String) confJsonObject.get("hbase_dev_push_apps");
	 	    
	 	    activeUsersPreprocess = (String) confJsonObject.get("hbase_activeUsers_preprocess");
	 	    activeUsersPreprocess_cf = (String) confJsonObject.get("hbase_activeUsers_preprocess_colfam");
	 	    activeUsers = (String) confJsonObject.get("hbase_activeUsers");
	 	    
	 	    newUsersPreprocess = (String) confJsonObject.get("hbase_newUsers_preprocess");
	 	    newUsers = (String) confJsonObject.get("hbase_newUsers");
	 	    
	 	    eventCount = (String) confJsonObject.get("hbase_table_events2");
	 	    eventsAU = (String) confJsonObject.get("hbase_table_events_AU");
	 	    eventsNU = (String) confJsonObject.get("hbase_table_events_NU");
	 	    
	 	    event_count_tab_name = (String) confJsonObject.get("hbase_events_count");
	 	    
	 	    //kafka settings
	 	    raw_iot_server_ip = (String) confJsonObject.get("server_ip");
	 	    raw_iot_spark_kafka_port = (String) confJsonObject.get("raw_iot_spark_kafka_port");
	 	    zk_node_conn_port = (String) confJsonObject.get("zk_node_conn_port");
	 	    raw_iot_zk_conn_timeout_milsec = (String) confJsonObject.get("raw_iot_zk_conn_timeout_milsec");
	 	    
	 	    kafka_consumer_id_secondary = (String) confJsonObject.get("raw_iot_kafka_cons_id_secondary");
	 	    kafka_consumer_group_id_secondary = (String) confJsonObject.get("raw_iot_kafka_cons_group_id_secondary");
	 	    
	 	    iot_spark_driver_memory = (String) confJsonObject.get("raw_iot_spark_driver_memory");
	 	    iot_streaming_block_size = (String) confJsonObject.get("raw_iot_streaming_block_size");
	 	    
	 	    props = new Properties();
			props.put("metadata.broker.list", raw_iot_server_ip + ":" + raw_iot_spark_kafka_port);
			props.put("kafka.consumer.id", kafka_consumer_id_secondary);
			props.put("group.id", kafka_consumer_group_id_secondary);
			props.put("zookeeper.connect", raw_iot_server_ip + ":" +  zk_node_conn_port);
			props.put("zookeeper.connection.timeout.ms", raw_iot_zk_conn_timeout_milsec);
			
			//avro deserialization
			iotRawDatumReader = new SpecificDatumReader<RcvdData>(RcvdData.getClassSchema());
			
			//spark settings
			sparkConf = new SparkConf().setAppName("The InThings")
						.setMaster("local[6]")
						.set("spark.scheduler.mode", "FAIR")
						.set("spark.sql.crossJoin.enabled", "true")
						.set("spark.driver.memory", iot_spark_driver_memory)
			    		.set("spark.streaming.blockInterval", iot_streaming_block_size)
			    		.set("spark.driver.allowMultipleContexts", "true");
			
			jsc = new JavaSparkContext(sparkConf); 
			spark = SparkSession.builder().appName("InThings Batch Processing")
					.config("spark.scheduler.mode", "FAIR")
					.config("spark.sql.crossJoin.enabled", "true")
					.config("spark.sql.crossJoin.enabled", true).getOrCreate();
			
			
			// settings for writing to push adid table
	 	    pushKeyAdIdJob = Job.getInstance(hbaseConf);
	 	    pushKeyAdIdJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, dev_map_apps);
	 	    pushKeyAdIdJob.setOutputFormatClass(TableOutputFormat.class);
	 	    
	 	    // settings for push appsecret table
	 	    pushDevAppsecJob = Job.getInstance(hbaseConf);
	 	    pushDevAppsecJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, push_appsecret_dev);
	 	    pushDevAppsecJob.setOutputFormatClass(TableOutputFormat.class);
	 	    
	 	    // settings for active users preprocess and query tables
	 	    activeUsersPreprocessJob = Job.getInstance(hbaseConf);
	 	    activeUsersPreprocessJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, activeUsersPreprocess);
	 	    activeUsersPreprocessJob.setOutputFormatClass(TableOutputFormat.class);
	 	    
	 	    // settings for active users table
	 	    activeUsersJob = Job.getInstance(hbaseConf);
	 	    activeUsersJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, activeUsers);
	 	    activeUsersJob.setOutputFormatClass(TableOutputFormat.class);
			
	 	    // settings for new users preprocess query
	 	    newUsersPreprocessJob = Job.getInstance(hbaseConf);
	 	    newUsersPreprocessJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, newUsersPreprocess);
	 	    newUsersPreprocessJob.setOutputFormatClass(TableOutputFormat.class);
	 	    
	 	    // settings for new users table
	 	    newUsersJob = Job.getInstance(hbaseConf);
	 	    newUsersJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, newUsers);
	 	    newUsersJob.setOutputFormatClass(TableOutputFormat.class);
	 	    
	 	    //events count
	 	    newAPIJobConfigEventsDBEvents = Job.getInstance(hbaseConf);
			newAPIJobConfigEventsDBEvents.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, event_count_tab_name);
			newAPIJobConfigEventsDBEvents.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
			
		} catch(IOException e) {
			e.printStackTrace();
		} catch(ParseException p) {
			p.printStackTrace();
		}
	}
	
	private void executeUserQueries() {
		Dataset<Row> iotSecondaryDS = inthings.loadIoTSecondary();
		if(iotSecondaryDS.count() > 0) {
			iotSecondaryDS.createOrReplaceTempView("iotSecondary");
			
			Dataset<Row> activeUsersPreprocessDS = hbaseUtilsObj.getActiveUsersPreprocessHbase(jsc, spark);
			activeUsersPreprocessDS.createOrReplaceTempView("activeUsersPreprocess");
			
			//collects rowkey, appsecret, deviceId, date, year, month, day for new users preprocess
			Dataset<Row> newUsersPreprocessDS1 = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, '__', "
									+ " from_unixtime(a.timeStamp, 'Y'), '__', from_unixtime(a.timeStamp, 'M'), '__', "
									+ " from_unixtime(a.timeStamp, 'd')) AS rowKey, a.iotRawDataAppSecret AS appSecret, "
									+ " a.iotRawDataDeviceId AS deviceId, a.timeStamp AS timeStamp, from_unixtime(a.timeStamp) AS date "
									+ " FROM iotSecondary AS a "
									+ " WHERE a.iotRawDataDeviceId NOT IN (SELECT b.iotRawDataDeviceId FROM activeUsersPreprocess AS b) AND "
									+ " CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, '__', from_unixtime(a.timeStamp, 'Y'), '__', "
									+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) IS NOT NULL "
									+ " AND a.iotRawDataAppSecret IS NOT NULL").distinct();
			
			System.out.println("<---------------------- NEW USERS PREPROCESS ------------------------>");
			
		    JavaRDD<Row> newUsersPreprocessRDD = newUsersPreprocessDS1.javaRDD();
			//new users preprocess dump into hbase
		    JavaPairRDD<ImmutableBytesWritable, Put> NUPreprocessToPut = newUsersPreprocessRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row newUsersPreObj) throws Exception {
					logger.info("iot New Users Preprocess push to hbase--->" + newUsersPreObj.getString(0) + " -- " + newUsersPreObj.getString(1) + " -- " 
									+ newUsersPreObj.getString(2) + " -- " + newUsersPreObj.getLong(3) + " -- " + newUsersPreObj.getString(4));
					return hbaseUtilsObj.newUsersPreprocess(newUsersPreObj.getString(0), newUsersPreObj.getString(1), newUsersPreObj.getString(2), 
															newUsersPreObj.getLong(3), newUsersPreObj.getString(4)); 
					}
				});
				
			if(NUPreprocessToPut.count() > 0) {
				NUPreprocessToPut.saveAsNewAPIHadoopDataset(newUsersPreprocessJob.getConfiguration());
				logger.info("$$$$$$$$$$$$$$$$$$ SAVED NEW USERS PREPROCESS DATA IN HBASE $$$$$$$$$$$$$$$$$$$$$$");
			}
			
			//contains rowkey, appsecret and count of new users based on everyday
			Dataset<Row> newUsersPreprocessDS2 = hbaseUtilsObj.getNewUsersPreprocessHbase(jsc, spark);
			newUsersPreprocessDS2.createOrReplaceTempView("newUsersPreprocess");
			
			Dataset<Row> newUsersDS = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timeStamp, 'Y'), '__', "
									+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) AS rowKey, "
									+ " a.iotRawDataAppSecret AS appSecret, COUNT(DISTINCT a.iotRawDataDeviceId) AS newUsers FROM newUsersPreprocess a "
									+ " WHERE "
									+ " CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, '__',from_unixtime(a.timeStamp, 'Y'), '__', "
									+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) IS NOT NULL "
									+ " AND a.iotRawDataDeviceId IS NOT NULL "
									+ " GROUP BY a.iotRawDataAppSecret, CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timeStamp, 'Y'), '__', "
									+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd'))").distinct();
			
			JavaRDD<Row> newUsersRDD = newUsersDS.javaRDD();
			//new users  put to hbase
			JavaPairRDD<ImmutableBytesWritable, Put> NUDataToPut = newUsersRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row newUsersObj) throws Exception {
					logger.info("iot New Users data to hbase--->" + newUsersObj.getString(0) + "--" + newUsersObj.getString(1) + "--" + newUsersObj.getLong(2));
					return hbaseUtilsObj.getNewUsers(newUsersObj.getString(0), newUsersObj.getString(1), newUsersObj.getLong(2));
				}
			});
			
			if(NUDataToPut.count() > 0) {
				NUDataToPut.saveAsNewAPIHadoopDataset(newUsersJob.getConfiguration());
				logger.info("$$$$$$$$$$$$$$$$$$ SAVED NEW USERS DATA IN HBASE $$$$$$$$$$$$$$$$$$$$$$");
			}
			
			activeUsersPreprocessDS = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, '__', from_unixtime(a.timeStamp, 'Y'), "
									+ " '__', from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) AS rowKey, a.iotRawDataAppSecret AS appSecret, "
									+ " a.iotRawDataDeviceId AS deviceId, a.timeStamp AS timeStamp, from_unixtime(a.timeStamp) AS date "
									+ " FROM iotSecondary AS a "
									+ " WHERE "
									+ " CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, '__', from_unixtime(a.timeStamp, 'Y'), '__', "
									+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) IS NOT NULL "
									+ " AND a.iotRawDataDeviceId IS NOT NULL").distinct();

			JavaRDD<Row> activeUsersPreprocessRDD = activeUsersPreprocessDS.javaRDD();
			//active users preprocess to hbase
			JavaPairRDD<ImmutableBytesWritable, Put> AUPreprocessToPut = activeUsersPreprocessRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row activeUsersPreObj) throws Exception {
					logger.info("iot Active Users Preprocess push to hbase--->" + activeUsersPreObj.getString(0) + "--"
									+ activeUsersPreObj.getString(1) + "--" + activeUsersPreObj.getString(2) + "--" + activeUsersPreObj.getLong(3) + "--" 
									+ activeUsersPreObj.getString(4));
					return hbaseUtilsObj.activeUsersPreprocess(activeUsersPreObj.getString(0), activeUsersPreObj.getString(1), 
																activeUsersPreObj.getString(2), activeUsersPreObj.getLong(3), activeUsersPreObj.getString(4));
				}
			});

			if(AUPreprocessToPut.count() > 0) {
				AUPreprocessToPut.saveAsNewAPIHadoopDataset(activeUsersPreprocessJob.getConfiguration());
				logger.info("$$$$$$$$$$$$$$$$$$ SAVED ACTIVE USERS PREPROCESS DATA IN HBASE $$$$$$$$$$$$$$$$$$$$$$");
			}
			
			activeUsersPreprocessDS = hbaseUtilsObj.getActiveUsersPreprocessHbase(jsc, spark);
			activeUsersPreprocessDS.createOrReplaceTempView("activeUsersPreprocess");
			
			Dataset<Row> activeUsersDS = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timeStamp, 'Y'), '__', "
									+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) AS rowKey, a.iotRawDataAppSecret AS appSecret, "
									+ " COUNT(DISTINCT a.iotRawDataDeviceId) AS activeUsers FROM activeUsersPreprocess a "
									+ " WHERE "
									+ " a.iotRawDataAppSecret IS NOT NULL AND CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timeStamp, 'Y'), '__', "
									+ " from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) IS NOT NULL GROUP BY a.iotRawDataAppSecret, "
									+ " CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timeStamp, 'Y'), '__', from_unixtime(a.timeStamp, 'M'), '__', "
									+ " from_unixtime(a.timeStamp, 'd'))").distinct();
			
			JavaRDD<Row> activeUsersRDD = activeUsersDS.javaRDD();
			//active users put to hbase
			JavaPairRDD<ImmutableBytesWritable, Put> AUDataToPut = activeUsersRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row activeUsersObj) throws Exception {
					logger.info("iot Active Users push to hbase--->" + activeUsersObj.getString(0) + "--" + activeUsersObj.getString(1) + "--" 
								+ activeUsersObj.getLong(2));
					return hbaseUtilsObj.getActiveUsers(activeUsersObj.getString(0), activeUsersObj.getString(1), activeUsersObj.getLong(2));
				}
			});
			
			if(AUDataToPut.count() > 0) {
				AUDataToPut.saveAsNewAPIHadoopDataset(activeUsersJob.getConfiguration());
				logger.info("$$$$$$$$$$$$$$$$$$ SAVED ACTIVE USERS DATA IN HBASE $$$$$$$$$$$$$$$$$$$$$$");
			}
			
			// order by timestamp
			Dataset<Row> pushKeyDS = spark.sql("SELECT a.iotRawDataDeviceId, a.iotRawDataAppSecret, a.iotRawDataPUSHACTIONPUSHKEY "
									+ " FROM iotSecondary AS a "
									+ " INNER JOIN (SELECT iotRawDataDeviceId, iotRawDataAppSecret, MAX(timeStamp) AS maxTS "
									+ " FROM iotSecondary WHERE iotRawDataPUSHACTIONPUSHKEY IS NOT NULL AND iotRawDataDeviceId IS NOT NULL "
									+ " GROUP BY iotRawDataDeviceId, iotRawDataAppSecret) AS b "
									+ " ON (a.iotRawDataDeviceId = b.iotRawDataDeviceId) AND "
									+ " (a.iotRawDataAppSecret = b.iotRawDataAppSecret) AND "
									+ " (a.timeStamp = b.maxTS) WHERE a.iotRawDataPUSHACTIONPUSHKEY IS NOT NULL "
									+ " AND a.iotRawDataDeviceId IS NOT NULL").distinct();

			logger.info("num rows pushKeyDF key-->" + pushKeyDS.count());
		
			JavaRDD<Row> pushKeyRDD = pushKeyDS.javaRDD();
			// send  push key stuff to hbase
			JavaPairRDD<ImmutableBytesWritable, Put> pushDataToPut = pushKeyRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row pushRowObj) throws Exception {
					logger.info("iotPush to hbase--->" + pushRowObj.getString(0) + "--" + pushRowObj.getString(1) + "--" + pushRowObj.getString(2));
						return hbaseUtilsObj.cnvrtPushDevDatatoPut(pushRowObj.getString(0), pushRowObj.getString(1), pushRowObj.getString(2));
					}
				});
			
			if(pushDataToPut.count() > 0) {
				pushDataToPut.saveAsNewAPIHadoopDataset(pushKeyAdIdJob.getConfiguration());
			}
			
			// appsec push stuff to hbase
			JavaPairRDD<ImmutableBytesWritable, Put> pushDevAppDataToPut = pushKeyRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row pushRowObj) throws Exception {
					logger.info("iotPush appsec, dev to hbase--->" + pushRowObj.getString(0) + "--" + pushRowObj.getString(1) + "--" + pushRowObj.getString(2));
					return hbaseUtilsObj.cnvrtPushDatatoPut(pushRowObj.getString(0), pushRowObj.getString(1), pushRowObj.getString(2));
				}
			});
			
			if(pushDevAppDataToPut.count() > 0) {
				pushDevAppDataToPut.saveAsNewAPIHadoopDataset(pushDevAppsecJob.getConfiguration());
			}
		    
		} else {
			logger.info("***************************** SKIPPING USER QUERIES SINCE SECONDARY TABLE IS EMPTY *********************************");
		}
		
	}
	
	private Dataset<Row> loadIoTSecondary() {
		//instantiate the list of rowKeys to be later deleted
		deletionRowKeys = new ArrayList<Delete>();
		
		//added devcie_model here!!
		hbaseConf.set(TableInputFormat.INPUT_TABLE, iot_secondary);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, iot_secondary_cf); // column family 
		String hbaseReadColList = iot_secondary_cf+":app_secret " +
								iot_secondary_cf+":packet_id " +
								iot_secondary_cf+":device_id " +
								iot_secondary_cf+":server_time " +
								iot_secondary_cf+":device_model" +
								iot_secondary_cf+":UID__IMEI " +
								iot_secondary_cf+":UID__WIFI_MAC_ADDRESS " +
								iot_secondary_cf+":UID__PSEUDO_UNIQUE_ID " +
								iot_secondary_cf+":AdIDActionAdIDKey " +
								iot_secondary_cf+":PushActnPushKey ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); 	// column qualifiers
   
		// read data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = 
											jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
		// filter out all null valued data
		//Get those bundles which have appSecret in the hbase tab
		JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDDFiltered = hBaseRDD.filter(
				new Function<Tuple2<ImmutableBytesWritable, Result>, Boolean>() {
					public Boolean call(Tuple2<ImmutableBytesWritable, Result> hbaseData) throws IOException {
						Result r = hbaseData._2;
						if (Bytes.toString(r.getValue(Bytes.toBytes(iot_secondary_cf), Bytes.toBytes("app_secret"))) == null){
							return false;
						} else {
							return true;
						}
					}
				}
		);
   
		// properly format the data into a new RDD 
		JavaRDD<IotRawDataBean> iotJavaRDD = hBaseRDDFiltered.map(
							new Function<Tuple2<ImmutableBytesWritable, Result>, IotRawDataBean>() {
								public IotRawDataBean call(
										Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
									// get the rowid and set it as key in the pairRDD
									Result r = entry._2;
									String keyRow = Bytes.toString(r.getRow());
									// define java bean  
									iot = new IotRawDataBean();
									
									deletionRowKeys.add(new Delete(r.getRow()));
									
									// set values from hbase
									iot.setiotRawDataRowKey(keyRow);
									iot.setiotRawDataAppSecret((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("app_secret"))));
									iot.setiotRawDataPacketId((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("packet_id"))));
									iot.setiotRawDataDeviceId((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("device_id"))));
									iot.setiotRawDataServerTime(Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("server_time"))));
									iot.setiotRawDataIMEI((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("UID__IMEI"))));
									iot.setiotRawDataWFMac((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("UID__WIFI_MAC_ADDRESS"))));
									iot.setiotRawDataUniqId((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("UID__PSEUDO_UNIQUE_ID"))));
									iot.setiotRawDataAdvertisingIdActionADVERTISINGIDKEY((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("AdIDActionAdIDKey"))));
									iot.setiotRawDataPUSHACTIONPUSHKEY((String) Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("PushActnPushKey"))));
									//set timestamp
									iot.settimeStamp(Long.parseLong(Bytes.toString(
											r.getValue(Bytes.toBytes(iot_secondary_cf), 
													Bytes.toBytes("server_time")))));
									return iot;
								}
							}
		);
		
		Dataset<Row> iotSecondaryDS = spark.createDataFrame(iotJavaRDD, IotRawDataBean.class);
		return iotSecondaryDS;
		
	}
	
	private void deleteIoTSecondaryData() {
		try {
			Scan scan = new Scan();
			deletionRowKeys = new ArrayList<Delete>();
			secondaryTable = new HTable(hbaseConf, iot_secondary);
			ResultScanner rs = secondaryTable.getScanner(scan);
			for(Result result : rs) {
				Delete delete = new Delete(result.getRow());
				deletionRowKeys.add(delete);
				logger.info("+++++++++++++++++++++++++++ added rowKey to delete from " + iot_secondary + ": " + Bytes.toString(result.getRow()));
			
			}
			
			secondaryTable.delete(deletionRowKeys);
			
		} catch(IOException e) {
			e.printStackTrace();
		}
		
	}
	
	private void dumpDataInElasticSearch(String hbaseTable, String hbaseTable_cf, String index) {
		hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTable);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseTable_cf); // column family
		String hbaseReadColList = hbaseTable_cf + ":rowKey " + hbaseTable_cf + ":app_secret " + hbaseTable_cf + ":device_id " +hbaseTable_cf + ":timestamp " 
								+ hbaseTable_cf + ":date " + hbaseTable_cf + ":year " + hbaseTable_cf + ":month " + hbaseTable_cf + ":day ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> AUPrePairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
				 																		ImmutableBytesWritable.class, Result.class);
		JavaRDD<ESDataBean> ElasticSearchRDD = null;
		
		if(hbaseTable_cf.equalsIgnoreCase(activeUsersPreprocess_cf)) {
			ElasticSearchRDD = AUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, ESDataBean>() {
				public ESDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
					Result r = entry._2;
					esbean = new ESDataBean();
					esbean.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("app_secret"))));
					esbean.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("device_id"))));
					esbean.setiotRawDataDeviceModel(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("device_model"))));
					esbean.setTimestamp(Bytes.toLong(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("timestamp"))));
					esbean.setDate(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("date"))));
					
					return esbean;
				}
			});
		}
		
		JavaEsSpark.saveToEs(ElasticSearchRDD, "/iotUsers/" + index);
		logger.info("<------------------- succesfully dumped data of " + hbaseTable + " in ElasticSearch ----------------->");
	}
	
	public static void main(String[] args) throws Exception {
		inthings = new BatchProcess();
		EventDB eventDB = new EventDB();
		while(true) {
			
			inthings.executeUserQueries();
			HbaseTableEvents.executeEventWiseQueries(eventCount, eventsAU, eventsNU, hbaseConf, jsc, spark);
			eventDB.getEventData(hbaseConf, jsc, spark);
			
			if(deletionRowKeys.size() > 0) {
				inthings.deleteIoTSecondaryData();
				deletionRowKeys.clear();
			}
			
			//EventsCount.getEventsCountData(hbaseConf, jsc);
			//logger.info("********************** DONE WITH EVENTS COUNT PROCESSING ***********************");
			//EventParamCount.getEventParamCount(hbaseConf, jsc);
			//logger.info("******************** DONE WITH EVENTS PARAM COUNT PROCESSING *******************");
			
		}
		
	}
	
}
