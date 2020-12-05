package com.example.helloworld;


import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Utf8;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;



@SpringBootApplication
public class HelloworldApplication {

	public static void main(String[] args) {

		SpringApplication.run(HelloworldApplication.class, args);

		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		Pipeline p = Pipeline.create(options);


		Schema avroSchema = new Schema.Parser().parse(
				"{\"type\": \"record\", "
						+ "\"name\": \"client\", "
						+ "\"fields\": ["
						+ "{\"name\": \"id\", \"type\": \"long\"},"
						+ "{\"name\": \"name\", \"type\": \"string\"}"
						+ "{\"name\": \"phone\", \"type\": \"string\"},"
						+ "{\"name\": \"address\", \"type\": \"string\"}"
						+ "]}");
		PCollection<GenericRecord> avroRecords = p.apply(
				AvroIO.readGenericRecords(avroSchema).from("gs://bucket/clients.avro"));


		PCollection<TableRow> bigQueryRows = avroRecords.apply(
				MapElements.into(TypeDescriptor.of(TableRow.class))
						.via(
								(GenericRecord elem) ->
										new TableRow()
												.set("id", ((Utf8) elem.get("id")).toString())
												.set("name", ((Utf8) elem.get("name")).toString())
												.set("phone", ((Utf8) elem.get("phone")).toString())
												.set("address", ((Utf8) elem.get("address")).toString())));

		TableSchema bigQuerySchema =
				new TableSchema()
						.setFields(
								ImmutableList.of(new TableFieldSchema()
										.setName("id")
										.setType("LONG"), new TableFieldSchema()
										.setName("name")
										.setType("STRING"), new TableFieldSchema()
										.setName("phone")
										.setType("STRING"), new TableFieldSchema()
				                        .setName("address")
				                        .setType("STRING")));

		bigQueryRows.apply(BigQueryIO.writeTableRows()
				.to(new TableReference()
						.setProjectId("backendservice")
						.setDatasetId("myDataSet")
						.setTableId("avro_source"))
				.withSchema(bigQuerySchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run().waitUntilFinish();

	}

}
