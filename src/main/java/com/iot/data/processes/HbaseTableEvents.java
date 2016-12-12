package com.iot.data.processes;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iot.data.schema.PacketData;
import com.iot.data.schema.RcvdData;
import com.iot.data.schema.SerializableIotData;
import com.iot.data.utils.HbaseUtils;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import scala.Tuple2;

@SuppressWarnings("deprecation")
public class HbaseTableEvents {
	//misc variables
	private static ObjectMapper mapper;
	
	//conf file variables
	private static String confFileName;
	private static String confTypeFile;
	private static JSONParser confParser = new JSONParser();
	private static Object confObj;
	private static JSONObject confJsonObject;
	
	//kafka variables
	private static Properties props;
	private static String kafka_consumer_id_events;
	private static String kafka_consumer_group_id_events;
	private static String kafka_iot_main_topic;
	private static String raw_iot_server_ip;
	private static String raw_iot_spark_kafka_port;
	private static String zk_node_conn_port;
	private static String raw_iot_zk_conn_timeout_milsec;
	private static int NO_OF_THREADS;
	
	//avro variables
	private static DatumReader<RcvdData> iotRawDatumReader;
	private static Decoder iotDecoder;
	
	//hbase variables
	private static String hbaseTableEvents1;
	private static String hbaseTableEvents1_colfam;
	private static Configuration hbaseIotConf;
	private static Put p;
	private static String hbase_master_port;
	private static String hbase_zookeeper_port;
	private static HTable hbaseTabEvents;
	
	private static HbaseUtils hbaseUtilsObj;
	private final static Logger logger = LoggerFactory.getLogger(HbaseTableEvents.class);
	
	static {
		try {
			//json related
			mapper = new ObjectMapper();
			
			//conf file settings
			confTypeFile = "production_conf.json";
			confFileName = "/home/iot/fsociety/dal/conf/" + confTypeFile;
			confObj = confParser.parse(new FileReader(confFileName));
	        confJsonObject = (JSONObject) confObj;
	        raw_iot_server_ip = (String) confJsonObject.get("server_ip");
	        raw_iot_spark_kafka_port = (String) confJsonObject.get("raw_iot_spark_kafka_port");
	        zk_node_conn_port = (String) confJsonObject.get("zk_node_conn_port");
	        raw_iot_zk_conn_timeout_milsec = (String) confJsonObject.get("raw_iot_zk_conn_timeout_milsec");
	        hbase_master_port = (String) confJsonObject.get("hbase_master_port");
			hbase_zookeeper_port = (String) confJsonObject.get("hbase_zookeeper_port");
	       
	        //hbase settings
	        hbaseTableEvents1 = (String) confJsonObject.get("hbase_table_events1");
	        hbaseTableEvents1_colfam = (String) confJsonObject.get("hbase_table_events1_colfam");
	        
	        //avro deserialization
	        iotRawDatumReader = new SpecificDatumReader<RcvdData>(RcvdData.getClassSchema());
	        
	        hbaseUtilsObj = new HbaseUtils(confTypeFile);
	        
	        //kafka and zookeeper settings
	        kafka_iot_main_topic = (String) confJsonObject.get("kafka_iot_main_topic");
	        kafka_consumer_id_events = (String)confJsonObject.get("raw_iot_kafka_cons_id_events");
			kafka_consumer_group_id_events = (String)confJsonObject.get("raw_iot_kafka_cons_group_id_events");
	        props = new Properties();
			props.put("metadata.broker.list", raw_iot_server_ip + ":" + raw_iot_spark_kafka_port);
			props.put("kafka.consumer.id", kafka_consumer_id_events);
			props.put("group.id", kafka_consumer_group_id_events);
			props.put("zookeeper.connect", raw_iot_server_ip + ":" +  zk_node_conn_port);
			props.put("zookeeper.connection.timeout.ms", raw_iot_zk_conn_timeout_milsec);
			NO_OF_THREADS = 1;
	        
			//hbase settings
			hbaseIotConf = HBaseConfiguration.create();
		    hbaseIotConf.set("hbase.master", raw_iot_server_ip + ":" + hbase_master_port);
		    hbaseIotConf.set("hbase.zookeeper.quorum", raw_iot_server_ip);
		    hbaseIotConf.set("hbase.zookeeper.property.clientPort", hbase_zookeeper_port);
		    
		    hbaseTabEvents = new HTable(hbaseIotConf, hbaseTableEvents1);
		} catch(IOException e) {
			e.printStackTrace();
		} catch(ParseException p) {
			p.printStackTrace();
		}
	}
	
	private void setupEventsConsumer() {
		try {
			while(true) {
				System.out.println("Started Kafka Consumer to listen on topic " + kafka_iot_main_topic + "...");
				ConsumerConfig consumerConf = new ConsumerConfig(props);
				ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConf);
	 	
				Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
				topicCountMap.put(kafka_iot_main_topic, new Integer(NO_OF_THREADS));
	 		
				Map<String, List<KafkaStream<byte[], byte[]>>> iotStreamMap = consumer.createMessageStreams(topicCountMap);
				List<KafkaStream<byte[], byte[]>> iotStreams = iotStreamMap.get(kafka_iot_main_topic);
				KafkaStream<byte[], byte[]> iotRawStream = iotStreams.get(0);
	 	
				ConsumerIterator<byte[], byte[]> iterator = iotRawStream.iterator();
	 		
				while(iterator.hasNext()) {
					byte[] avroObject = iterator.next().message();
					iotDecoder = DecoderFactory.get().binaryDecoder(avroObject, null);
					RcvdData avroDataPacket = iotRawDatumReader.read(null, iotDecoder);
					SerializableIotData iotDataPacket = new SerializableIotData(avroDataPacket);
	 				logger.info("iot data packet received in EVENTS =======> " + iotDataPacket.getAppSecret() + "__" + iotDataPacket.getPacketId());
	 				
	 				//dump data in Events table
	 				dumpInEventsHbaseTable(iotDataPacket);
				}
			}
		} catch(IOException e) {
			e.printStackTrace();
		}	
	}
	
	@SuppressWarnings("deprecation")
	private static void dumpInEventsHbaseTable(SerializableIotData iotDataPacket) {
		try {
			int ctr = 1;
			String currentRowKey, eventPropJSON;
		
			for(PacketData currPacket : iotDataPacket.getPacket()) {
				//events data
				if(currPacket.getEvents() != null) {
					currentRowKey = iotDataPacket.getPacketId() + "__" + ctr;
					ctr++;
					//put the data
					p = new Put(Bytes.toBytes(currentRowKey));
					//add corresponding events data to events preprocess table
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("packet_id"), Bytes.toBytes(iotDataPacket.getPacketId()));
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("app_secret"), Bytes.toBytes(iotDataPacket.getAppSecret()));
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("device_id"), Bytes.toBytes(iotDataPacket.getDeviceId()));
					p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("server_time"), Bytes.toBytes(iotDataPacket.getServerTime()));
					
					if(currPacket.getEvents() != null) {
						if(currPacket.getEvents().getEvent() != null) {
							p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("event_name"), Bytes.toBytes(currPacket.getEvents().getEvent()));
						}
					}
			
					if(currPacket.getEvents().getProperties() != null) {
						Map<String, String> eventProps = new HashMap<String, String>();
						eventProps = currPacket.getEvents().getProperties();
						eventPropJSON = mapper.writeValueAsString(eventProps);
						if(eventPropJSON != null) {
							p.add(Bytes.toBytes(hbaseTableEvents1_colfam), Bytes.toBytes("event_properties"), Bytes.toBytes(eventPropJSON));
						}
					}
					
					hbaseTabEvents.put(p);
				}
			}
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Job configureEventsJobs(String table, Configuration hbaseConf) throws IOException {
		Job job = Job.getInstance(hbaseConf);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		job.setOutputFormatClass(TableOutputFormat.class);
		
		return job;
	}
	
	public static void executeEventWiseQueries(String eventCountTable, String eventsAU, String eventsNU, Configuration hbaseConf, JavaSparkContext jsc, 
													SparkSession spark) throws IOException {
		Dataset<Row> events1DS = hbaseUtilsObj.getEvents1HbaseTable(jsc, spark, hbaseIotConf);
		
		if(events1DS.count() > 0) {
			events1DS.createOrReplaceTempView("events1");
			//cumulative event wise count
			Dataset<Row> events2DS = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.iotRawDataServerTime, 'Y'), '__', "
									+ " from_unixtime(a.iotRawDataServerTime, 'M'), '__', from_unixtime(a.iotRawDataServerTime, 'd')) as rowkey, "
									+ " a.iotRawDataAppSecret AS appsecret, a.iotRawDataEventName AS eventname, "
									+ " COUNT(DISTINCT a.iotRawDataDeviceId) AS eventCount FROM events1 AS a "
									+ " WHERE "
									+ " a.iotRawDataEventName IS NOT NULL "
									+ " AND CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.iotRawDataServerTime, 'Y'), '__', "
									+ " from_unixtime(a.iotRawDataServerTime, 'M'), '__', from_unixtime(a.iotRawDataServerTime, 'd')) IS NOT NULL "
									+ " GROUP BY CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.iotRawDataServerTime, 'Y'), '__', "
									+ " from_unixtime(a.iotRawDataServerTime, 'M'), '__', from_unixtime(a.iotRawDataServerTime, 'd')), "
									+ " a.iotRawDataAppSecret, a.iotRawDataEventName");
			
			Job eventCountJob = configureEventsJobs(eventCountTable, hbaseConf);
			JavaRDD<Row> events2RDD = events2DS.javaRDD();
			JavaPairRDD<ImmutableBytesWritable, Put> events2PairRDD = events2RDD.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row events2DSObj) throws Exception {
					logger.info("iot cumulative event count query push to hbase ---> " + events2DSObj.getString(0), events2DSObj.getString(1), 
									events2DSObj.getString(2), events2DSObj.getLong(3));
					return hbaseUtilsObj.getCumulativeEventsCount(events2DSObj.getString(0), events2DSObj.getString(1), events2DSObj.getString(2), 
																	events2DSObj.getLong(3));
				}
			});
			
			if(events2PairRDD.count() > 0) {
				events2PairRDD.saveAsNewAPIHadoopDataset(eventCountJob.getConfiguration());
			}
			
			Dataset<Row> activeUsersPreprocess = hbaseUtilsObj.getActiveUsersPreprocessHbase(jsc, spark);
			activeUsersPreprocess.createOrReplaceTempView("activeUsersPreprocess");
			
			Dataset<Row> eventwiseAUds1 = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timestamp, 'Y'), '__', "
										+ " from_unixtime(a.timestamp, 'M'), '__', from_unixtime(a.timestamp, 'd')) AS rowkey, "
										+ " a.iotRawDataAppSecret AS appsecret, a.iotRawDataDeviceId AS deviceId, a.iotRawDataEventName AS eventname "
										+ " FROM events1 AS a "
										+ " INNER JOIN activeUsersPreprocess AS b ON a.iotRawDataDeviceId = b.iotRawDataDeviceId "
										+ " AND a.iotRawDataAppSecret = b.iotRawDataAppSecret "
										+ " WHERE "
										+ " CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timestamp, 'Y'), '__', from_unixtime(a.timestamp, 'M'), '__', "
										+ " from_unixtime(a.timestamp, 'd')) IS NOT NULL AND a.iotRawDataDeviceId IS NOT NULL "
										+ " AND a.iotRawDataEventName IS NOT NULL").distinct();
			
			eventwiseAUds1.createOrReplaceTempView("eventsAU");
			
			Dataset<Row> eventwiseAUds2 = spark.sql("SELECT a.rowkey AS rowkey, a.appsecret AS appsecret, a.eventname AS eventname, "
										+ " COUNT(DISTINCT a.deviceId) AS activeUsers FROM eventsAU AS a WHERE a.eventname IS NOT NULL "
										+ " GROUP BY a.rowkey, a.appsecret, a.eventname");

			Job eventsAUJob = configureEventsJobs(eventsAU, hbaseConf);
			JavaRDD<Row> eventwiseAUrdd = eventwiseAUds2.javaRDD();
			JavaPairRDD<ImmutableBytesWritable, Put> eventwiseAUpairRDD = eventwiseAUrdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row eventwiseAUobj) throws Exception {
					logger.info("********************* IOT eventwise active users AU ------> "+ eventwiseAUobj.getString(0) + "--" + eventwiseAUobj.getString(1), 
									eventwiseAUobj.getString(2) + "--" + eventwiseAUobj.getLong(3));
					return hbaseUtilsObj.doEventwiseSegmentationAU(eventwiseAUobj.getString(0), eventwiseAUobj.getString(1), eventwiseAUobj.getString(2), 
																	eventwiseAUobj.getLong(3));
				}
			});

			if(eventwiseAUpairRDD.count() > 0) {
				eventwiseAUpairRDD.saveAsNewAPIHadoopDataset(eventsAUJob.getConfiguration());
			}
			
			//event wise new users calculation
			Dataset<Row> NUpreprocessDS = hbaseUtilsObj.getNewUsersPreprocessHbase(jsc, spark);
			NUpreprocessDS.createOrReplaceTempView("newUsersPreprocess");
			
			Dataset<Row> eventwiseNUds1 = spark.sql("SELECT CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timestamp, 'Y'), '__', "
										+ " from_unixtime(a.timestamp, 'M'), '__', from_unixtime(a.timestamp, 'd')) AS rowkey, "
										+ " a.iotRawDataAppSecret AS appsecret, a.iotRawDataDeviceId AS deviceId, a.iotRawDataEventName AS eventname"
										+ " FROM events1 AS a "
										+ " INNER JOIN newUsersPreprocess AS b ON a.iotRawDataDeviceId = b.iotRawDataDeviceId "
										+ " AND a.iotRawDataAppSecret = b.iotRawDataAppSecret "
										+ " WHERE "
										+ " CONCAT(a.iotRawDataAppSecret, '__', from_unixtime(a.timestamp, 'Y'), '__', from_unixtime(a.timestamp, 'M'), '__', "
										+ " from_unixtime(a.timestamp, 'd')) IS NOT NULL AND a.iotRawDataEventName IS NOT NULL "
										+ " AND a.iotRawDataDeviceId IS NOT NULL").distinct();
			
			eventwiseNUds1.createOrReplaceTempView("eventsNU");
			
			Dataset<Row> eventwiseNUds2 = spark.sql("SELECT a.rowkey AS rowkey, a.appsecret AS appsecret, a.eventname AS eventname, "
					+ " COUNT(DISTINCT a.deviceId) AS newUsers FROM eventsNU AS a GROUP BY a.rowkey, a.appsecret, a.eventname");

			Job eventsNUJob = configureEventsJobs(eventsNU, hbaseConf);
			JavaRDD<Row> eventwiseNUrdd = eventwiseNUds2.javaRDD();
			
			JavaPairRDD<ImmutableBytesWritable, Put> eventwiseNUpairRDD = eventwiseNUrdd.mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
				public Tuple2<ImmutableBytesWritable, Put> call(Row eventwiseNUobj) throws Exception {
					logger.info("iot eventwise new users NU ---> "+ eventwiseNUobj.getString(0), eventwiseNUobj.getString(1), 
									eventwiseNUobj.getString(2), eventwiseNUobj.getLong(3));
					return hbaseUtilsObj.doEventwiseSegmentationNU(eventwiseNUobj.getString(0), eventwiseNUobj.getString(1), eventwiseNUobj.getString(2), 
																eventwiseNUobj.getLong(3));
				}
			});
			
			if(eventwiseNUpairRDD.count() > 0) {
				eventwiseNUpairRDD.saveAsNewAPIHadoopDataset(eventsNUJob.getConfiguration());
			}
				
		} else {
			logger.info("***************************** SKIPPING EVENT-WISE USERS SINCE SECONDARY TABLE IS EMPTY *********************************");
		}
		
	}
	
	public static void main(String[] args) {
		
		HbaseTableEvents events = new HbaseTableEvents();	
		events.setupEventsConsumer();
		
	}
	
}