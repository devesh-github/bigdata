package com.dev.fileformats;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import parquet.avro.AvroParquetReader;
import parquet.avro.AvroSchemaConverter;
import parquet.avro.AvroWriteSupport;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;
/*import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;*/

public class ParquetExp {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		 	System.setProperty("hadoop.home.dir", "H:\\DevesH\\BigData\\Tutorials\\");
		
			//Parsing and Loadind a schema
			//final String schemaLocation = "H://DevesH//workspace_Java//BigData-Spark-Concepts//avro_format.avsc";
			final String schemaLocation = "./avro_format.avsc";
			Schema avroSchema = null;
			try {
				avroSchema = new Schema.Parser().parse(new File(schemaLocation));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			//System.out.println(avroSchema.getDoc());
			
			//Creating a Generic Record
			final GenericRecord record = new GenericData.Record(avroSchema);
			record.put("id", 1);
			record.put("age", 10);
			record.put("name", "ABC");
			record.put("place", "BCD");
			
			final MessageType parquetSchema = new AvroSchemaConverter().convert(avroSchema);
			final WriteSupport writeSupport = new AvroWriteSupport(parquetSchema, avroSchema);
			
			try {
	            Files.deleteIfExists(Paths.get("H://DevesH//workspace_Java//BigData-Spark-Concepts//data.parquet"));
	        } catch(NoSuchFileException e) {
	            System.out.println("No such file/directory exists");
	        } catch (IOException e) {
				e.printStackTrace();
			}
			
			final String parquetFile = "H://DevesH//workspace_Java//BigData-Spark-Concepts//data.parquet";
			final Path path = new Path(parquetFile);

			//Parquet Writer
			//ParquetWriter<GenericRecord> parquetWriter = new ParquetWriter(path, writeSupport, CompressionCodecName.SNAPPY, BLOCK_SIZE, PAGE_SIZE);
			ParquetWriter<GenericRecord> parquetWriter=null;
			try {
				parquetWriter = new ParquetWriter(path, writeSupport, CompressionCodecName.SNAPPY,ParquetWriter.DEFAULT_BLOCK_SIZE,ParquetWriter.DEFAULT_PAGE_SIZE);
				//parquetWriter = new ParquetWriter(path, writeSupport, CompressionCodecName.SNAPPY,128,128);
				parquetWriter.write(record);
				parquetWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			//Parquet Reader
			//AvroParquetReader<GenericRecord> reader = new AvroParquetReader.<GenericRecord>builder(parquetFile).build();
			AvroParquetReader<GenericRecord> reader = null;
			GenericRecord nextRecord = null;
			try {
				reader = new AvroParquetReader<GenericRecord>(path);
				nextRecord = reader.read();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Schema==>"+nextRecord.getSchema());
			System.out.println("Data===>"+nextRecord.get("age"));
			
	}
}
