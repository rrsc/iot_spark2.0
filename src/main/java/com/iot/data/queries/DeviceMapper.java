package com.iot.data.queries;

import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
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
import com.iot.data.utils.HbaseUtils;
import scala.Tuple2;

public class DeviceMapper {
	//misc settings
	private static Configuration hbaseConf;
	private static IotRawDataBean iot;
	private static ESDataBean esbean;
	private static SparkSession spark;
	private static SparkConf sparkConf;
	private static JavaSparkContext jsc;
	private static Job newAPIJobConfigIotPushAdid;
	private static Job newAPIJobConfigIotPushDevAppsec;
	private static Job newAPIJobConfigIotActiveUsersPreprocess;
	private static Job newAPIJobConfigIotActiveUsersQry;
	private static Job newAPIJobConfigIotNewUsersPreprocess;
	private static Job newAPIJobConfigIotNewUsersQry;
	private static Job newAPIJobConfigIotEventwiseCount;
	private static Job newAPIJobConfigIotEventwiseAU;
	private static Job newAPIJobConfigIotEventwiseNU;
	
	// conf file settings
	private static String confFileName;
	private static String confTypeFile;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	private static String hbase_master_ip;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	private static String raw_iot_tab_name;
	private static String raw_iot_tab_colfam;

	private static HbaseUtils hbaseUtilsObj;
	// hbase tabs 
	private static String hbaseTableEvents1;
	private static String hbaseTableEvents1_colfam;
	private static String hbaseTableEvents2;
	private static String hbaseTableEventsAU;
	private static String hbaseTableEventsNU;
	private static Put p;
	
	private static String dev_map_apps_tab_name;
	private static String push_appsecret_dev_tab_name;
	private static String active_users_preprocess_name;
	private static String active_users_preprocess_colfam;
	private static String active_users_tab_name;
	private static String new_users_preprocess_name;
	private static String new_users_preprocess_colfam;
	private static String new_users_tab_name;
	
	private final static Logger logger = LoggerFactory.getLogger(DeviceMapper.class);
	
	public DeviceMapper() throws IOException, ParseException {
		// settings for production or testing (choose one)
		confTypeFile = "production_conf.json";
		// ###################### CONF FILE TYPE ######################
		// read conf file and corresponding params
		confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
		// read the json file and create a map of the parameters
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
 	    // hbase table conf
 	    hbaseTableEvents1 = (String) confJsonObject.get("hbase_table_events1");
 	   	hbaseTableEvents1_colfam = (String) confJsonObject.get("hbase_table_events1_colfam");
 	   	hbaseTableEvents2 = (String) confJsonObject.get("hbase_table_events2");
 	    
 	    raw_iot_tab_name = (String) confJsonObject.get("hbase_table_primary");
        raw_iot_tab_colfam = (String) confJsonObject.get("hbase_raw_data_tab_colfam");
        dev_map_apps_tab_name = (String) confJsonObject.get("hbase_table_dev_map_apps");
        push_appsecret_dev_tab_name = (String) confJsonObject.get("hbase_dev_push_apps");
        active_users_preprocess_name = (String) confJsonObject.get("hbase_activeUsers_preprocess");
        active_users_preprocess_colfam = (String) confJsonObject.get("hbase_activeUsers_preprocess_colfam");
        active_users_tab_name = (String) confJsonObject.get("hbase_activeUsers");
        new_users_preprocess_name = (String) confJsonObject.get("hbase_newUsers_preprocess");
        new_users_preprocess_colfam = (String) confJsonObject.get("hbase_newUsers_preprocess_colfam");
        new_users_tab_name = (String) confJsonObject.get("hbase_newUsers");
        
        hbaseTableEventsAU = (String) confJsonObject.get("hbase_table_events_AU");
        hbaseTableEventsNU = (String) confJsonObject.get("hbase_table_events_NU");
        
        //spark settings
        sparkConf = new SparkConf().setAppName("queryDevs").setMaster("local[4]").set("spark.scheduler.mode", "FAIR")
        				.set("spark.sql.crossJoin.enabled", "true");
	    jsc = new JavaSparkContext(sparkConf); 
        spark = SparkSession.builder().appName("Hbase Table running for events").config("spark.scheduler.mode", "FAIR")
        				.config("spark.sql.crossJoin.enabled", "true").config("spark.sql.crossJoin.enabled", true).getOrCreate();
        
 	    // settings for writing to push adid table
        newAPIJobConfigIotPushAdid = Job.getInstance(hbaseConf);
        newAPIJobConfigIotPushAdid.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, dev_map_apps_tab_name);
        newAPIJobConfigIotPushAdid.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
 	    // settings for push appsecret table
        newAPIJobConfigIotPushDevAppsec = Job.getInstance(hbaseConf);
        newAPIJobConfigIotPushDevAppsec.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, push_appsecret_dev_tab_name);
        newAPIJobConfigIotPushDevAppsec.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
        
        // settings for active users preprocess and query tables
        newAPIJobConfigIotActiveUsersPreprocess = Job.getInstance(hbaseConf);
        newAPIJobConfigIotActiveUsersPreprocess.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, active_users_preprocess_name);
        newAPIJobConfigIotActiveUsersPreprocess.setOutputFormatClass(TableOutputFormat.class);
        
        newAPIJobConfigIotActiveUsersQry = Job.getInstance(hbaseConf);
        newAPIJobConfigIotActiveUsersQry.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, active_users_tab_name);
        newAPIJobConfigIotActiveUsersQry.setOutputFormatClass(TableOutputFormat.class);
        
        //settings for new users preprocess query
        newAPIJobConfigIotNewUsersPreprocess = Job.getInstance(hbaseConf);
        newAPIJobConfigIotNewUsersPreprocess.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, new_users_preprocess_name);
        newAPIJobConfigIotNewUsersPreprocess.setOutputFormatClass(TableOutputFormat.class);
        
        // settings for new users query tables
        newAPIJobConfigIotNewUsersQry = Job.getInstance(hbaseConf);
        newAPIJobConfigIotNewUsersQry.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, new_users_tab_name);
        newAPIJobConfigIotNewUsersQry.setOutputFormatClass(TableOutputFormat.class);
        
        //settings for eventwise overall count table
	    newAPIJobConfigIotEventwiseCount = Job.getInstance(hbaseConf);
	    newAPIJobConfigIotEventwiseCount.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseTableEvents2);
	    newAPIJobConfigIotEventwiseCount.setOutputFormatClass(TableOutputFormat.class);
	    
	    //settings for eventwise active users count table
	    newAPIJobConfigIotEventwiseAU = Job.getInstance(hbaseConf);
	    newAPIJobConfigIotEventwiseAU.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseTableEventsAU);
	    newAPIJobConfigIotEventwiseAU.setOutputFormatClass(TableOutputFormat.class);
	    
	    //settings for eventwise new users count table
	    newAPIJobConfigIotEventwiseNU = Job.getInstance(hbaseConf);
	    newAPIJobConfigIotEventwiseNU.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseTableEventsNU);
	    newAPIJobConfigIotEventwiseNU.setOutputFormatClass(TableOutputFormat.class);

	}
	
	public static void mapDevs() throws Exception {
			// set table and column names
			hbaseConf.set(TableInputFormat.INPUT_TABLE, raw_iot_tab_name);
			hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, raw_iot_tab_colfam); // column family 
			String hbaseReadColList = raw_iot_tab_colfam+":app_secret " +
					raw_iot_tab_colfam+":packet_id " +
					raw_iot_tab_colfam+":device_id " +
					raw_iot_tab_colfam+":server_time " +
					raw_iot_tab_colfam+":UID__IMEI " +
	    			raw_iot_tab_colfam+":UID__WIFI_MAC_ADDRESS " +
	    			raw_iot_tab_colfam+":UID__PSEUDO_UNIQUE_ID " +
	    			raw_iot_tab_colfam+":AdIDActionAdIDKey " +
	    			raw_iot_tab_colfam+":PushActnPushKey ";
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
							if (Bytes.toString(r.getValue(Bytes.toBytes(raw_iot_tab_colfam), Bytes.toBytes("app_secret"))) == null){
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
										// set values from hbase
										iot.setiotRawDataRowKey(keyRow);
										iot.setiotRawDataAppSecret((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("app_secret"))));
										iot.setiotRawDataPacketId((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("packet_id"))));
										iot.setiotRawDataDeviceId((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("device_id"))));
										iot.setiotRawDataServerTime((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("server_time"))));
										iot.setiotRawDataIMEI((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("UID__IMEI"))));
										iot.setiotRawDataWFMac((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("UID__WIFI_MAC_ADDRESS"))));
										iot.setiotRawDataUniqId((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("UID__PSEUDO_UNIQUE_ID"))));
										iot.setiotRawDataAdvertisingIdActionADVERTISINGIDKEY((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("AdIDActionAdIDKey"))));
										iot.setiotRawDataPUSHACTIONPUSHKEY((String) Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("PushActnPushKey"))));

										//set timestamp
										iot.settimeStamp(Long.parseLong(Bytes.toString(
												r.getValue(Bytes.toBytes(raw_iot_tab_colfam), 
														Bytes.toBytes("server_time")))));
										return iot;
									}
								}
			);
	   
			Dataset<Row> iotRawDS = spark.createDataFrame(iotJavaRDD, IotRawDataBean.class);
			iotRawDS.cache();
			iotRawDS.createOrReplaceTempView("iotTab");
	    
			//register temporary table for activeUsersPreprocess, by reading from hbase and dumping the data in dataframe and registering as temp table!!
			Dataset<Row> AUMasterPreDS = getAUPreprocessHbase();
			
			System.out.println("=========== ACTIVE USERS TABLE LOADED AND CREATED TEMPORARY DATASET TABLE ============");
			AUMasterPreDS.createOrReplaceTempView("activeUsersPreprocess");
	   
			//collects rowkey, appsecret, deviceId, date, year, month, day for new users preprocess
			Dataset<Row> newUsersPreDS = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, '__', from_unixtime(a.timeStamp, 'Y'), "
							+ " '__', from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) AS rowKey, a.iotRawDataAppSecret AS appSecret, "
							+ " a.iotRawDataDeviceId AS deviceId, a.timeStamp, from_unixtime(a.timeStamp) AS date, from_unixtime(a.timeStamp, 'Y') AS year, "
							+ " from_unixtime(a.timeStamp, 'M') AS month, from_unixtime(a.timeStamp, 'd') AS day "
							+ " FROM iotTab AS a "
							+ " WHERE a.iotRawDataDeviceId NOT IN (SELECT b.iotRawDataDeviceId FROM activeUsersPreprocess AS b)");
			
			System.out.println("================== NEW USERS PREPROCESS ====================");
		
			
			Dataset<Row> activeUsersPreDS = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', a.iotRawDataDeviceId, '__', from_unixtime(a.timeStamp, 'Y'), "
								+ "'__', from_unixtime(a.timeStamp, 'M'), '__', from_unixtime(a.timeStamp, 'd')) AS rowkey, a.iotRawDataAppSecret AS appSecret, "
								+ " a.iotRawDataDeviceId AS deviceId, a.timeStamp, from_unixtime(a.timeStamp) AS date, from_unixtime(a.timeStamp, 'Y') AS year, "
								+ " from_unixtime(a.timeStamp, 'M') AS month, from_unixtime(a.timeStamp, 'd') AS day FROM iotTab a");
			
			System.out.println("================== ACTIVE USERS PREPROCESS ====================");
			
			// order by timestamp
			Dataset<Row> pushKeyDS = spark.sql("SELECT a.iotRawDataDeviceId, a.iotRawDataAppSecret, iotRawDataPUSHACTIONPUSHKEY "
	    							+ " FROM iotTab as a "
						    	    + " INNER JOIN (SELECT iotRawDataDeviceId, iotRawDataAppSecret, MAX(timeStamp) as maxTS "
						    	    + " FROM iotTab WHERE iotRawDataPUSHACTIONPUSHKEY IS NOT NULL "
						    	    + " GROUP BY iotRawDataDeviceId, iotRawDataAppSecret) as b "
						    	    + " ON (a.iotRawDataDeviceId = b.iotRawDataDeviceId) AND "
						    	    + " (a.iotRawDataAppSecret = b.iotRawDataAppSecret) AND "
						    	    + " (a.timeStamp = b.maxTS) WHERE iotRawDataPUSHACTIONPUSHKEY IS NOT NULL");
			
			System.out.println("================== PUSH KEY DATASET =====================");
	    
			System.out.println("num rows pushKeyDF key--> " + pushKeyDS.count());
			System.out.println("num rows activeUsers Preprocess DF key--> " + activeUsersPreDS.count());
	    
			logger.info("==============================>");
			logger.info("num rows pushKeyDF key-->" + pushKeyDS.count());
			logger.info("num rows activeUsers Preprocess DF key-->" + activeUsersPreDS.count());
			logger.info("==============================>");
	    
			JavaRDD<Row> pushRDD = pushKeyDS.javaRDD();
			JavaRDD<Row> actvUsrsPreRDD = activeUsersPreDS.javaRDD();
			//JavaRDD<Row> actvUsrsRDD = activeUsersDS.javaRDD();
			JavaRDD<Row> newUsrsPreRDD = newUsersPreDS.javaRDD();
			//JavaRDD<Row> newUsersRDD = newUsersDS.javaRDD();
    
			// send  push key stuff to hbase
			JavaPairRDD<ImmutableBytesWritable, Put> pushDataToPut = pushRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row pushRowObj) throws Exception {
					logger.info("iotPush to hbase--->" + pushRowObj.getString(0) + "--" + pushRowObj.getString(1) + "--" + pushRowObj.getString(2));
					return hbaseUtilsObj.cnvrtPushDevDatatoPut(pushRowObj.getString(0), pushRowObj.getString(1), pushRowObj.getString(2));
				}
			});
			pushDataToPut.saveAsNewAPIHadoopDataset(newAPIJobConfigIotPushAdid.getConfiguration());
	    
			// appsec push stuff to hbase
			JavaPairRDD<ImmutableBytesWritable, Put> pushDevAppDataToPut = pushRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row pushRowObj)
						throws Exception {
					logger.info("iotPush appsec, dev to hbase--->" + pushRowObj.getString(0) 
							+ "--" + pushRowObj.getString(1) + "--" + pushRowObj.getString(2));
					return hbaseUtilsObj.cnvrtPushDatatoPut(pushRowObj.getString(0), pushRowObj.getString(1), pushRowObj.getString(2)); 
				}
			});
			pushDevAppDataToPut.saveAsNewAPIHadoopDataset(newAPIJobConfigIotPushDevAppsec.getConfiguration());
	  
			//active users preprocess to hbase
			/**JavaPairRDD<ImmutableBytesWritable, Put> AUPreprocessToPut = actvUsrsPreRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row activeUsersObj) throws Exception {
					logger.info("iot Active Users Preprocess push to hbase--->" + activeUsersObj.getString(0) 
							+ "--" + activeUsersObj.getString(1) + "--" + activeUsersObj.getString(2) + "--" + activeUsersObj.getLong(3)
							+ "--" + activeUsersObj.getString(4) + "--" + activeUsersObj.getString(5) + "--" + activeUsersObj.getString(6)
							+ "--" + activeUsersObj.getString(7));
					return hbaseUtilsObj.activeUsersPreprocess(activeUsersObj.getString(0), activeUsersObj.getString(1), activeUsersObj.getString(2), 
							activeUsersObj.getLong(3), activeUsersObj.getString(4));
				}
			});
			AUPreprocessToPut.saveAsNewAPIHadoopDataset(newAPIJobConfigIotActiveUsersPreprocess.getConfiguration());
	    
	    
			//new users preprocess dump into hbase
			JavaPairRDD<ImmutableBytesWritable, Put> NUPreprocessToPut = newUsrsPreRDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row newUsersObj) throws Exception {
					logger.info("iot New Users Preprocess push to hbase--->" + newUsersObj.getString(0), newUsersObj.getString(1), 
						newUsersObj.getString(2), newUsersObj.getLong(3), newUsersObj.getString(4), newUsersObj.getString(5), 
						newUsersObj.getString(6), newUsersObj.getString(7));
					return hbaseUtilsObj.newUsersPreprocess(newUsersObj.getString(0), newUsersObj.getString(1), newUsersObj.getString(2), 
						newUsersObj.getLong(3), newUsersObj.getString(4)); 
				}
			});
			NUPreprocessToPut.saveAsNewAPIHadoopDataset(newAPIJobConfigIotNewUsersPreprocess.getConfiguration());**/
	    
	    
			//dump into ElasticSearch here!
			//baar baar rdd from activeUsersPreprocess table, ES mein data how many times?????
			
		/*hbaseConf.set(TableInputFormat.INPUT_TABLE, active_users_preprocess_name);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, active_users_preprocess_colfam); // column family
		String hbaseReadColList = active_users_preprocess_colfam + ":rowKey " +
								active_users_preprocess_colfam + ":app_secret " +
								active_users_preprocess_colfam + ":device_id " +
								active_users_preprocess_colfam + ":timestamp " +
								active_users_preprocess_colfam + ":date " +
								active_users_preprocess_colfam + ":year " +
								active_users_preprocess_colfam + ":month " +
								active_users_preprocess_colfam + ":day ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> AUPrePairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
				 															ImmutableBytesWritable.class, Result.class);
		
		JavaRDD<ESDataBean> AUPreES = AUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, ESDataBean>() {
			public ESDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowkey = Bytes.toString(r.getRow());
				esbean = new ESDataBean();
				esbean.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("app_secret"))));
				esbean.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("device_id"))));
				esbean.setDate(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("timestamp"))));
				esbean.setDate(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("date"))));
				esbean.setYear(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("year"))));
				esbean.setMonth(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("month"))));
				esbean.setDay(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("day"))));
				return esbean;
			}
		});
		
			JavaEsSpark.saveToEs(AUPreES, "/spark/activeUsersPreprocess");*/
			
			//part for events cumulative count and AU/NU  wise segmentation
			executeEventsQueries();
	}
	
	//fetch active users preprocess data from hbase into rdd
	public static Dataset<Row> getAUPreprocessHbase() {
		hbaseConf.set(TableInputFormat.INPUT_TABLE, active_users_preprocess_name);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, active_users_preprocess_colfam); // column family
		String hbaseReadColList = active_users_preprocess_colfam + ":rowKey " +
								active_users_preprocess_colfam + ":app_secret " +
								active_users_preprocess_colfam + ":device_id " +
								active_users_preprocess_colfam + ":timestamp " +
								active_users_preprocess_colfam + ":date " +
								active_users_preprocess_colfam + ":year " +
								active_users_preprocess_colfam + ":month " +
								active_users_preprocess_colfam + ":day ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> AUPrePairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
				 															ImmutableBytesWritable.class, Result.class);
		JavaRDD<IotRawDataBean> AUPreprocessRDD = AUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, IotRawDataBean>() {
			public IotRawDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowkey = Bytes.toString(r.getRow());
				iot = new IotRawDataBean();
				iot.setiotRawDataRowKey(rowkey);
				iot.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("app_secret"))));
				iot.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("device_id"))));
				iot.settimeStamp(Bytes.toLong(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("timestamp"))));
				iot.setDate(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("date"))));
				iot.setYear(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("year"))));
				iot.setMonth(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("month"))));
				iot.setDay(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("day"))));
				
				return iot;
			}
		});
		
		Dataset<Row> AUPreprocessDS = spark.createDataFrame(AUPreprocessRDD, IotRawDataBean.class);
		return AUPreprocessDS;
	}
	
	public static Dataset<Row> getNUPreprocessHbase() {
		hbaseConf.set(TableInputFormat.INPUT_TABLE, new_users_preprocess_name);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, new_users_preprocess_colfam); // column family
		String hbaseReadColList = new_users_preprocess_colfam + ":rowKey " +
							new_users_preprocess_colfam + ":app_secret " +
							new_users_preprocess_colfam + ":device_id " +
							new_users_preprocess_colfam + ":timestamp " +
							new_users_preprocess_colfam + ":date " +
							new_users_preprocess_colfam + ":year " +
							new_users_preprocess_colfam + ":month " +
							new_users_preprocess_colfam + ":day ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> NUPrePairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
						 															ImmutableBytesWritable.class, Result.class);
		JavaRDD<IotRawDataBean> NUPreprocessRDD = NUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, IotRawDataBean>() {
			public IotRawDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowkey = Bytes.toString(r.getRow());
				iot = new IotRawDataBean();
				iot.setiotRawDataRowKey(rowkey);
				iot.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("app_secret"))));
				iot.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("device_id"))));
				iot.settimeStamp(Bytes.toLong(r.getValue(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("timestamp"))));
				iot.setDate(Bytes.toString(r.getValue(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("date"))));
				iot.setYear(Bytes.toString(r.getValue(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("year"))));
				iot.setMonth(Bytes.toString(r.getValue(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("month"))));
				iot.setDay(Bytes.toString(r.getValue(Bytes.toBytes(new_users_preprocess_colfam), Bytes.toBytes("day"))));
				
				return iot;
			}
		});
		
		Dataset<Row> NUPreprocessDS = spark.createDataFrame(NUPreprocessRDD, IotRawDataBean.class);
		return NUPreprocessDS;	
	}
	
	private static void executeEventsQueries() throws InterruptedException {
		Dataset<Row> events1DS = getEvents1HbaseTable();
		events1DS.createOrReplaceTempView("events1");
		
		//cumulative event wise count
		Dataset<Row> events2DS = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timestamp, 'Y'), '__', "
									+ " from_unixtime(a.timestamp, 'M'), '__', from_unixtime(a.timestamp, 'd')) as rowkey, "
									+ " a.iotRawDataAppSecret AS appsecret, a.iotRawDataEventName AS eventname, "
									+ " COUNT(DISTINCT a.iotRawDataDeviceId) AS eventCount FROM events1 AS a GROUP BY CONCAT(a.iotRawDataAppSecret, '__', "
									+ " from_unixtime(a.timestamp, 'Y'), '__', from_unixtime(a.timestamp, 'M'), '__', from_unixtime(a.timestamp, 'd')), "
									+ " a.iotRawDataAppSecret, a.iotRawDataEventName");
		
		JavaRDD<Row> events2RDD = events2DS.javaRDD();
		
		JavaPairRDD<ImmutableBytesWritable, Put> events2PairRDD = events2RDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
			public Tuple2<ImmutableBytesWritable, Put> call(Row events2DSObj) throws Exception {
				logger.info("iot cumulative event count query push to hbase ---> " + events2DSObj.getString(0), events2DSObj.getString(1), 
								events2DSObj.getString(2), events2DSObj.getLong(3));
				return hbaseUtilsObj.getCumulativeEventsCount(events2DSObj.getString(0), events2DSObj.getString(1), events2DSObj.getString(2), 
																events2DSObj.getLong(3));
			}
		});
		events2PairRDD.saveAsNewAPIHadoopDataset(newAPIJobConfigIotEventwiseCount.getConfiguration());
		
		//event wise active users calculation
		Dataset<Row> AUpreprocessDS = DeviceMapper.getAUPreprocessHbase();
		AUpreprocessDS.createOrReplaceTempView("activeUsersPreprocess");
		
		Dataset<Row> eventwiseAUds1 = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timestamp, 'Y'), '__', "
											+ " from_unixtime(a.timestamp, 'M'), '__', from_unixtime(a.timestamp, 'd')) AS rowkey, "
											+ " a.iotRawDataAppSecret AS appsecret, a.iotRawDataDeviceId AS deviceId, a.iotRawDataEventName AS eventname "
											+ " FROM events1 AS a "
											+ " INNER JOIN activeUsersPreprocess AS b ON a.iotRawDataDeviceId = b.iotRawDataDeviceId "
											+ " AND a.iotRawDataAppSecret = b.iotRawDataAppSecret").distinct();
		
		eventwiseAUds1.createOrReplaceTempView("eventsAU");
		
		Dataset<Row> eventwiseAUds2 = spark.sql("SELECT a.rowkey AS rowkey, a.appsecret AS appsecret, a.eventname AS eventname, "
									+ " COUNT(DISTINCT a.deviceId) AS activeUsers FROM eventsAU AS a WHERE a.eventname IS NOT NULL "
									+ " GROUP BY a.rowkey, a.appsecret, a.eventname");
		
		JavaRDD<Row> eventwiseAUrdd = eventwiseAUds2.javaRDD();
		
		JavaPairRDD<ImmutableBytesWritable, Put> eventwiseAUpairRDD = eventwiseAUrdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
			public Tuple2<ImmutableBytesWritable, Put> call(Row eventwiseAUobj) throws Exception {
				logger.info("iot eventwise active users AU ---> "+ eventwiseAUobj.getString(0), eventwiseAUobj.getString(1), 
								eventwiseAUobj.getString(2), eventwiseAUobj.getLong(3));
				return hbaseUtilsObj.doEventwiseSegmentationAU(eventwiseAUobj.getString(0), eventwiseAUobj.getString(1), eventwiseAUobj.getString(2), 
																eventwiseAUobj.getLong(3));
			}
		});
		eventwiseAUpairRDD.saveAsNewAPIHadoopDataset(newAPIJobConfigIotEventwiseAU.getConfiguration());
		
		//event wise new users calculation
		Dataset<Row> NUpreprocessDS = DeviceMapper.getNUPreprocessHbase();
		NUpreprocessDS.createOrReplaceTempView("newUsersPreprocess");
		
		Dataset<Row> eventwiseNUds1 = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timestamp, 'Y'), '__', "
									+ " from_unixtime(a.timestamp, 'M'), '__', from_unixtime(a.timestamp, 'd')) AS rowkey, "
									+ " a.iotRawDataAppSecret AS appsecret, a.iotRawDataDeviceId AS deviceId, a.iotRawDataEventName AS eventname"
									+ " FROM events1 AS a "
									+ " INNER JOIN newUsersPreprocess AS b ON a.iotRawDataDeviceId = b.iotRawDataDeviceId "
									+ " AND a.iotRawDataAppSecret = b.iotRawDataAppSecret").distinct();
		
		eventwiseNUds1.createOrReplaceTempView("eventsNU");
		
		Dataset<Row> eventwiseNUds2 = spark.sql("SELECT a.rowkey AS rowkey, a.appsecret AS appsecret, a.eventname AS eventname, "
									+ " COUNT(DISTINCT a.deviceId) AS newUsers FROM eventsNU AS a GROUP BY a.rowkey, a.appsecret, a.eventname");
		
		JavaRDD<Row> eventwiseNUrdd = eventwiseNUds2.javaRDD();
		
		JavaPairRDD<ImmutableBytesWritable, Put> eventwiseNUpairRDD = eventwiseNUrdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
			public Tuple2<ImmutableBytesWritable, Put> call(Row eventwiseNUobj) throws Exception {
				logger.info("iot eventwise new users NU ---> "+ eventwiseNUobj.getString(0), eventwiseNUobj.getString(1), 
								eventwiseNUobj.getString(2), eventwiseNUobj.getLong(3));
				return hbaseUtilsObj.doEventwiseSegmentationNU(eventwiseNUobj.getString(0), eventwiseNUobj.getString(1), eventwiseNUobj.getString(2), 
															eventwiseNUobj.getLong(3));
			}
		});
		eventwiseNUpairRDD.saveAsNewAPIHadoopDataset(newAPIJobConfigIotEventwiseNU.getConfiguration());
		
	}
	
	private static Dataset<Row> getEvents1HbaseTable() {
		hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableEvents1);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseTableEvents1_colfam);	//column family
		String hbaseReadColList = hbaseTableEvents1_colfam + ":rowKey " +
				hbaseTableEvents1_colfam + ":app_secret " +
				hbaseTableEvents1_colfam + ":device_id " +
				hbaseTableEvents1_colfam + ":timestamp " +
				hbaseTableEvents1_colfam + ":event_name " +
				hbaseTableEvents1_colfam + ":event_properties ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList);
		
		JavaPairRDD<ImmutableBytesWritable, Result> events1PairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
																			ImmutableBytesWritable.class, Result.class);
		JavaRDD<IotRawDataBean> events1RDD = events1PairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, IotRawDataBean>() {
			public IotRawDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowKey = Bytes.toString(r.getRow());
				iot = new IotRawDataBean();
				iot.setiotRawDataRowKey(rowKey);
				iot.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("app_secret"))));
				iot.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("device_id"))));
				iot.setiotRawDataServerTime(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("timestamp"))));
				iot.setiotRawDataEventName(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("event_name"))));
				iot.setiotRawDataEventProperties(Bytes.toString(r.getValue(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("event_properties"))));
				return iot;
			}
		});
		
		Dataset<Row> events1DS = spark.createDataFrame(events1RDD, IotRawDataBean.class);
		return events1DS;
	}
	
	@SuppressWarnings("deprecation")
	private static Tuple2<ImmutableBytesWritable, Put> putEventWiseCount(String colfamily, String rowkey, String appsecret, String eventName, long eventCount) {
		//put the data
		p = new Put(Bytes.toBytes(rowkey));
		//dump event wise count data to hbase
		p.add(Bytes.toBytes(colfamily), Bytes.toBytes("app_secret"), Bytes.toBytes(appsecret));
		p.add(Bytes.toBytes(colfamily), Bytes.toBytes(eventName), Bytes.toBytes(eventCount));
		
		//return the put object
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), p);
	}
	
	public static void validateElasticSearch() {
		
		hbaseConf.set(TableInputFormat.INPUT_TABLE, active_users_preprocess_name);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, active_users_preprocess_colfam); // column family
		String hbaseReadColList = active_users_preprocess_colfam + ":rowKey " +
								active_users_preprocess_colfam + ":app_secret " +
								active_users_preprocess_colfam + ":device_id " +
								active_users_preprocess_colfam + ":timestamp " +
								active_users_preprocess_colfam + ":date " +
								active_users_preprocess_colfam + ":year " +
								active_users_preprocess_colfam + ":month " +
								active_users_preprocess_colfam + ":day ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> AUPrePairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
				 															ImmutableBytesWritable.class, Result.class);
		
		JavaRDD<ESDataBean> AUPreES = AUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, ESDataBean>() {
			public ESDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				String rowkey = Bytes.toString(r.getRow());
				esbean = new ESDataBean();
				esbean.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("app_secret"))));
				esbean.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("device_id"))));
				esbean.setTimestamp(Bytes.toLong(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("timestamp"))));
				esbean.setDate(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("date"))));
				//esbean.setYear(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("year"))));
				//esbean.setMonth(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("month"))));
				//esbean.setDay(Bytes.toString(r.getValue(Bytes.toBytes(active_users_preprocess_colfam), Bytes.toBytes("day"))));
				return esbean;
			}
		});
		
			JavaEsSpark.saveToEs(AUPreES, "/activeusers/activeusers");
			System.out.println("================== SUCCESSFULLY DUMPED DATA IN ELASTICSEARCH ====================");
		
	}
	  
	public static void main(String[] args) throws Exception {
		  DeviceMapper devObj=new DeviceMapper();
		//  while(true) {
			//devObj.mapDevs();
		//  }
		  
		  validateElasticSearch();
		  
	}
	
}