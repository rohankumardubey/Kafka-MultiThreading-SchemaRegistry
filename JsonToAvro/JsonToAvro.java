package com.modeln.intg.client.SamplePocCode.JsonToAvro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;


public class JsonToAvro {
    public static byte[] MasterworkJasonToAvro(String json, String MasterSchema) throws Exception {
        //converting the json String to bytes of streams and using ByteArrayInputStream to read byte array
        InputStream input = new ByteArrayInputStream(json.getBytes());
        //read primitive data from the input stream in a machine-independent way.
        DataInputStream din = new DataInputStream(input);

        //getting the schema
        Schema schema = Schema.parse(MasterSchema);

        //Utilities for Decoding Avro data.
        //Decoderfactory provides two types of decoders: binary decoder and JSON decoder
        //JSON decoder: An object that decodes instances of a data type from JSON format data.
        //DecoderFactory.get() : Returns an immutable static DecoderFactory configured with default settings
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

        //use this to read data on a given Schema
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        //read the data from the respective schema
        Object datum = reader.read(null, decoder);

        //use this to write data on a given Schema
        GenericDatumWriter<Object> Write = new GenericDatumWriter<>(schema);
        //converting the json String to bytes of streams and using ByteArrayOutputStream to write byte array
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        //EncoderFactory.get() : Returns an immutable static EncoderFactory configured with default settings
        //EncoderFactory provides two types of encoders: binary encoder and JSON encoder
        //Utilities for encoding Avro data.
        Encoder encode = EncoderFactory.get().binaryEncoder(outputStream, null);
        //writes the schema and data
        Write.write(datum, encode);
        encode.flush();
        return outputStream.toByteArray();
    }

    public static <Genericrecord> void main(String[] args) throws Exception {
        String MasterSchema = "{ \"type\" : \"record\", \"name\" : \"whatsapp_schema\", \"namespace\" : \"com.learning.kafka\", \"fields\" : [ { \"name\" : \"username\", \"type\" : \"string\", \"doc\"  : \"username of account\" }, { \"name\" : \"password\", \"type\" : \"string\", \"doc\"  : \"password of the account\" }, { \"name\" : \"email\", \"type\" : \"string\", \"doc\"  : \"email of the account\" } ], \"doc:\" : \"schema for storing the user details\" }";
        String json = "{\"username\":\"rohan\",\"password\":\"rohankumar\",\"email\": \"rohankumar@gmail.com\" }";

        byte[] avroByteArray = JsonToAvro.MasterworkJasonToAvro(json, MasterSchema);
        Schema schema = Schema.parse(MasterSchema);

        //use this to read data on a given Schema
        DatumReader<Genericrecord> reader1 = new GenericDatumReader<>(schema);

        //Utilities for Decoding Avro data.
        //Decoderfactory provides two types of decoders: binary decoder and JSON decoder
        //binaryDecoder: An object that decodes instances of a data type from binary format data.
        //DecoderFactory.get() : Returns an immutable static DecoderFactory configured with default settings
        Decoder decoder1 = DecoderFactory.get().binaryDecoder(avroByteArray, null);

        //A generic instance of a record schema. Fields are accessible by name as well as by index.
        GenericRecord result = (GenericRecord) reader1.read(null, decoder1);

        System.out.println(result.get("username").toString());
        System.out.println(result.get("password").toString());
        System.out.println(result.get("email").toString());
    }
}