package com.iot.data.schema;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class SerializableIotData extends RcvdData implements Serializable {
	private void setValues(RcvdData iotData) {
		setAppSecret(iotData.getAppSecret());
		setDeviceId(iotData.getDeviceId());
		setLibrary(iotData.getLibrary());
		setLibraryVersion(iotData.getLibraryVersion());
		setServerIp(iotData.getServerIp());
		setUNIQUEIDACTION(iotData.getUNIQUEIDACTION());
		setPacket(iotData.getPacket());
		setServerTime(iotData.getServerTime());
		setPacketId(iotData.getPacketId());
    }
	
	public SerializableIotData(RcvdData iotData) {
        setValues(iotData);
    }
	
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        DatumWriter<RcvdData> writer = new SpecificDatumWriter<RcvdData>(RcvdData.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(this, encoder);
        encoder.flush();
    }
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        DatumReader<RcvdData> reader = new SpecificDatumReader<RcvdData>(RcvdData.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        setValues(reader.read(null, decoder));
    }
	
	@SuppressWarnings("unused")
	private void readObjectNoData() throws ObjectStreamException {}

}