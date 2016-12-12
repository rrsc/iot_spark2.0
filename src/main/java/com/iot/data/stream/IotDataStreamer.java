package com.iot.data.stream;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.iot.data.schema.RcvdData;
import com.iot.data.schema.SerializableIotData;
import com.iot.data.utils.HbaseUtils;
import scala.Tuple2;
// hbase imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class IotDataStreamer {
	
	// conf file settings
	private static String confFileName;
	private static String confTypeFile;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	// spark settings
	private SparkConf sparkConf;
	private static JavaStreamingContext jssc;
	private static JavaSparkContext javasc;
	private static int numThreadsTopics;
	private static HashMap<String, String> props;
	private static String iot_app_name;
	private static String raw_iot_master_val;
	private static String raw_iot_spark_serializer;
	private static String raw_iot_spark_driver_memory;
	private static Long raw_iot_streaming_batch_size;
	private static String raw_iot_streaming_block_size;
	private static String raw_iot_spark_checkpoint_dir;
	private static String raw_iot_tab_name;
	private static String raw_iot_tab_colfam;
	// kafka/zookeeper settings
	private static String raw_iot_server_ip;
	private static String raw_iot_spark_kafka_port;
	private static String raw_iot_kafka_cons_group_id;
	private static String raw_iot_kafka_cons_id;
	private static String zk_node_conn_port;
	private static String raw_iot_zk_conn_timeout_milsec;
	private static String kafka_iot_main_topic;
	// avro schema 
	private static DatumReader<RcvdData> iotRawDatumReader;
	private static Decoder iotDecoder;
	// hbase settings
	private static Configuration hbaseConf;
	private static Job newAPIJobConfigIotRaw;
	private static String hbase_master_ip;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	private static HbaseUtils hbaseUtilsObj;
	
	private final static Logger logger = LoggerFactory
			.getLogger(IotDataStreamer.class);
	
	public IotDataStreamer() throws IOException, ParseException {
		// ###################### CONF FILE TYPE ######################
		// ###################### CONF FILE TYPE ######################
		// settings for production or testing (choose one)
		confTypeFile = "production_conf.json";
		// ###################### CONF FILE TYPE ######################
		// ###################### CONF FILE TYPE ######################
		// read conf file and corresponding params
		confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
		// read the json file and create a map of the parameters
		confObj = confParser.parse(new FileReader(confFileName));
        confJsonObject = (JSONObject) confObj;
        // read parameters from conf file
        iot_app_name = (String) confJsonObject.get("raw_iot_stream_app_name");
        raw_iot_master_val = (String) confJsonObject.get("raw_iot_master_val");
        raw_iot_spark_serializer = (String) confJsonObject.get("raw_iot_spark_serializer");
        raw_iot_spark_driver_memory = (String) confJsonObject.get("raw_iot_spark_driver_memory");
        raw_iot_streaming_batch_size = (Long) confJsonObject.get("raw_iot_streaming_batch_size");
        raw_iot_streaming_block_size = (String) confJsonObject.get("raw_iot_streaming_block_size");
        raw_iot_server_ip = (String) confJsonObject.get("server_ip");
        raw_iot_spark_kafka_port = (String) confJsonObject.get("raw_iot_spark_kafka_port");
        raw_iot_tab_name = (String) confJsonObject.get("hbase_table_primary");
        raw_iot_kafka_cons_id = (String) confJsonObject.get("raw_iot_kafka_cons_id_primary");
        raw_iot_kafka_cons_group_id = (String) confJsonObject.get("raw_iot_kafka_cons_group_id_primary");
        zk_node_conn_port = (String) confJsonObject.get("zk_node_conn_port");
        raw_iot_zk_conn_timeout_milsec = (String) confJsonObject.get("raw_iot_zk_conn_timeout_milsec");
        kafka_iot_main_topic = (String) confJsonObject.get("kafka_iot_main_topic");
        raw_iot_spark_checkpoint_dir = (String) confJsonObject.get("raw_iot_spark_checkpoint_dir");
        raw_iot_tab_colfam = (String) confJsonObject.get("hbase_raw_data_tab_colfam");
        hbase_master_ip = (String) confJsonObject.get("server_ip");
        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
        hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
        // avro deserialization
        iotRawDatumReader = new SpecificDatumReader<RcvdData>(RcvdData.getClassSchema());
		// set spark conf
		sparkConf = new SparkConf().setAppName(iot_app_name)
				.setMaster(raw_iot_master_val)
	    		.set("spark.serializer", raw_iot_spark_serializer)
	    		.set("spark.driver.memory", raw_iot_spark_driver_memory)
	    		.set("spark.streaming.blockInterval", raw_iot_streaming_block_size)
				.set("spark.driver.allowMultipleContexts", "true")
				.set("spark.scheduler.mode", "FAIR");
	    // Create the context with the given batch size
	    jssc = new JavaStreamingContext(sparkConf, new Duration(raw_iot_streaming_batch_size));
	    javasc = new JavaSparkContext(sparkConf);	    
	    // spark streaming props
	    props = new HashMap<String, String>();
	    props.put("metadata.broker.list", raw_iot_server_ip+":"+raw_iot_spark_kafka_port);
		props.put("kafka.consumer.id", raw_iot_kafka_cons_id);
		props.put("group.id", raw_iot_kafka_cons_group_id);
		props.put("zookeeper.connect", raw_iot_server_ip + ":" +  zk_node_conn_port);
		props.put("zookeeper.connection.timeout.ms", raw_iot_zk_conn_timeout_milsec);
		// conf for reading from/ writing to hbase
 		hbaseConf = HBaseConfiguration.create();
	    hbaseConf.set("hbase.master",hbase_master_ip + ":" + hbase_master_port);
	    hbaseConf.set("hbase.zookeeper.quorum", hbase_master_ip);
	    hbaseConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);
	    // settings for writing to iot raw data table
	    newAPIJobConfigIotRaw = Job.getInstance(hbaseConf);
	    newAPIJobConfigIotRaw.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, raw_iot_tab_name);
	    newAPIJobConfigIotRaw.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
	    hbaseUtilsObj = new HbaseUtils(confTypeFile);
	}
	
	
	@SuppressWarnings("deprecation")
	public void getIotRawData() throws ClassNotFoundException, IOException {
		// Make a topic map and assign threads to the topics
		// Currently we have 1 topic and 1 thread assigned to it.
		try {
			int numThreads = numThreadsTopics;
			// we replaced the "createStream" method used below with "createDirectStream"
		    // method - fault tolerance is better with "createDirectStream".
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
		    String[] topics = kafka_iot_main_topic.split(",");
		    for (String topic: topics) {
		    	//System.out.println("1 ********"+topic);
		        topicMap.put(topic, numThreads);
		    }
		    String topicsDirct = kafka_iot_main_topic;
		    HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topicsDirct.split(",")));
		    
		    
		    JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(
		            jssc,
		            String.class,
		            byte[].class,
		            StringDecoder.class,
		            DefaultDecoder.class,
		            props,
		            topicsSet
		        );
		    
		    // get the iot raw data objects, deserialize them using the (Java) Serializable
		    // interface (rather than the default avro class/interface). We do this as avro schema classes
		    // don't implement the Serializable interface of Java and are not directly compatible 
		    // with Spark.
		    JavaDStream<SerializableIotData> allIoTData = messages.map(new Function<Tuple2<String, byte[]>, SerializableIotData>() {
		        public SerializableIotData call(Tuple2<String, byte[]> tuple2) throws ClassNotFoundException, IOException {
		        	// Deserialize the data from avro format
		        	iotDecoder = DecoderFactory.get().binaryDecoder(tuple2._2, null);
		        	RcvdData avroDataPacket= iotRawDatumReader.read(null, iotDecoder);
		        	SerializableIotData iotDataPacket = new SerializableIotData(avroDataPacket);
		        	logger.info("iotDataPacket: " + avroDataPacket.getAppSecret() + "__" + avroDataPacket.getDeviceId());
		            return iotDataPacket;
		        }
		      });
		    
		    //works with spark version 1.6
		  /**  allIoTData.foreachRDD(new Function<JavaRDD<SerializableIotData>, Void>(){ 
		    	public Void call(JavaRDD<SerializableIotData> iotRawList) throws IOException {
		    		if (iotRawList.count() > 0) {
		    			// store data in clicks table
		    			JavaPairRDD<ImmutableBytesWritable, Put> putIotRawData = iotRawList.mapToPair(new PairFunction<SerializableIotData, ImmutableBytesWritable, Put>() {
							public Tuple2<ImmutableBytesWritable, Put> call(SerializableIotData iotRawDataObj)
									throws Exception {
								logger.info("iotDataPacket to hbase--->" + iotRawDataObj.getAppSecret() + "__" + iotRawDataObj.getDeviceId());
								
								return hbaseUtilsObj.cnvrtIotRawDataToPut(iotRawDataObj);
							}
						});
		    			putIotRawData.saveAsNewAPIHadoopDataset(newAPIJobConfigIotRaw.getConfiguration());
		    		}
					return null;
		      }});*/
		    
		    // put all the raw data in hbase w.r.t spark 2.0.0
		    allIoTData.foreachRDD(new VoidFunction<JavaRDD<SerializableIotData>>() {
				public void call(JavaRDD<SerializableIotData> iotRawList) throws Exception {
					if (iotRawList.count() > 0) {
						// store data in clicks table
						JavaPairRDD<ImmutableBytesWritable, Put> putIotRawData = iotRawList.mapToPair(
																			new PairFunction<SerializableIotData, ImmutableBytesWritable, Put>() {
							public Tuple2<ImmutableBytesWritable, Put> call(SerializableIotData iotRawDataObj)
									throws Exception {
								logger.info("iotDataPacket to hbase--->" + iotRawDataObj.getAppSecret() + "__" + iotRawDataObj.getDeviceId());
								
								return hbaseUtilsObj.cnvrtIotRawDataToPut(iotRawDataObj);
							}
						});
		    			putIotRawData.saveAsNewAPIHadoopDataset(newAPIJobConfigIotRaw.getConfiguration());
					}
				}
			});
		    
		} catch (Exception e) {
	        e.printStackTrace();  
	    }  
	    
	    System.out.println("Spark Streaming started....");
	    jssc.checkpoint(raw_iot_spark_checkpoint_dir);
	    jssc.start();
	   // jssc.awaitTermination();
	    System.out.println("Stopped Spark Streaming");
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, ParseException {		
		IotDataStreamer iotStrmObj = new IotDataStreamer();
		iotStrmObj.getIotRawData();
	
	}

}