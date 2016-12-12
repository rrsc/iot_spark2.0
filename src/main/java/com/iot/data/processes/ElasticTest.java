package com.iot.data.processes;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.iot.data.databeans.ESDataBean;
import com.iot.data.utils.HbaseUtils;

import scala.Tuple2;

public class ElasticTest implements Serializable {
	private static String confTypeFile,confFileName,hbase_master_ip,hbase_master_port,hbase_zookeeper_port,activeUsersPreprocess, activeUsersPreprocess_cf;
	//private static String raw_iot_server_ip, raw_iot_spark_kafka_port, zk_node_conn_port, raw_iot_zk_conn_timeout_milsec, kafka_iot_main_topic, NO_OF_THREADS;
	private static Properties props;
	private static JSONParser confParser;
	private static Object confObj;
	private static JSONObject confJsonObject;
	private static Configuration hbaseConf;
	private static HbaseUtils hbaseUtilsObj;
	private static SparkConf sparkConf;
	private static JavaSparkContext jsc;
	private static SparkSession spark;
	private static ESDataBean esbean;
	
	public ElasticTest() throws org.json.simple.parser.ParseException, IOException {
		//conf file settings
				confTypeFile = "production_conf.json";
				confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
				//confFileName = "/Users/sahil/Desktop/random/conf/" + confTypeFile;
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
		 	    
		 	    
		 	    activeUsersPreprocess = (String) confJsonObject.get("hbase_activeUsers_preprocess");
		 	    activeUsersPreprocess_cf = (String) confJsonObject.get("hbase_activeUsers_preprocess_colfam");
		 	    
				//spark settings
				sparkConf = new SparkConf().setAppName("The InThings").setMaster("local[4]").set("spark.scheduler.mode", "FAIR").set("spark.sql.crossJoin.enabled", "true");
				jsc = new JavaSparkContext(sparkConf); 
				spark = SparkSession.builder().appName("InThings Batch Processing").config("spark.scheduler.mode", "FAIR")
												.config("spark.sql.crossJoin.enabled", "true").config("spark.sql.crossJoin.enabled", true).getOrCreate();
		
	}
	
	public void dumpInElasticsearch(String hbaseTable, String hbaseTable_cf, String index) {
		hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTable);
		hbaseConf.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseTable_cf); // column family
		String hbaseReadColList = hbaseTable_cf + ":rowKey " + hbaseTable_cf + ":app_secret " + hbaseTable_cf + ":device_id " +hbaseTable_cf + ":timestamp " 
								+ hbaseTable_cf + ":date " + hbaseTable_cf + ":year " + hbaseTable_cf + ":month " + hbaseTable_cf + ":day ";
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, hbaseReadColList); // column qualifiers
		//read the hbase data into rdd
		JavaPairRDD<ImmutableBytesWritable, Result> AUPrePairRDD = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, 
				 																		ImmutableBytesWritable.class, Result.class);
		JavaRDD<ESDataBean> ElasticSearchRDD = null;
		
		ElasticSearchRDD = AUPrePairRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, ESDataBean>() {
			public ESDataBean call(Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {
				Result r = entry._2;
				esbean = new ESDataBean();
				esbean.setiotRawDataAppSecret(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("app_secret"))));
				esbean.setiotRawDataDeviceId(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("device_id"))));
				//esbean.setiotRawDataDeviceModel(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("device_model"))));
				esbean.setTimestamp(Bytes.toLong(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("timestamp"))));
				esbean.setDate(Bytes.toString(r.getValue(Bytes.toBytes(activeUsersPreprocess_cf), Bytes.toBytes("date"))));
				
				return esbean;
			}
		});
		
		JavaEsSpark.saveToEs(ElasticSearchRDD, "/activeusers/" + index);
		
	}
	
	public static void main(String[] args) throws ParseException, IOException {
		
		ElasticTest testObject = new ElasticTest();
		testObject.dumpInElasticsearch(activeUsersPreprocess, activeUsersPreprocess_cf, "activeUsers");
		
	}
	
}
