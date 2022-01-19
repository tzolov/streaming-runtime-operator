package com.vmware.tanzu.streaming.runtime.throwaway;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamStatusStorageAddressServers;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecPayloadSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

public class MainYamlSchema {
	public static void main(String[] args) throws IOException {

		System.out.println("Local dir = " + new File(".").getAbsolutePath());

//		 CREATE TABLE Songs (
//            `the_kafka_key` STRING,
//            `id` BIGINT NOT NULL,
//            `album` STRING,
//            `artist` STRING,
//            `name` STRING,
//            `genre` STRING NOT NULL,
//            `proctime` AS PROCTIME()
//		 ) WITH (
//		   'connector' = 'kafka', // protocol
//			'topic' = '${sql.aggregation.songsTopic}', // stream.name
//			'properties.bootstrap.servers' = '${sql.aggregation.kafkaServer}', // stream.status....
//			'properties.group.id' = 'testGroup', // FIXED
//			'key.format' = 'raw',           // FIXED
//			'key.fields' = 'the_kafka_key', // FIXED
//			'value.format' = 'avro-confluent',       // FIXED ? Confluent only if registory uri exist
//			'value.avro-confluent.url' = '${sql.aggregation.schemaRegistry}', // stream ???
//			'value.fields-include' = 'EXCEPT_KEY',   // FIXED
//			'scan.startup.mode' = 'earliest-offset'  // FIXED
//		)

		V1alpha1Stream stream = toV1alpha1Stream("file:./streaming-runtime/src/test/java/com/vmware/tanzu/streaming/runtime/throwaway/SongStream.yaml");
		createKafkaTableDdl(stream);

//		CREATE TABLE PlayEvents (
//            `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', //TODO
//            `the_kafka_key` STRING,
//            `song_id` BIGINT NOT NULL,
//            `duration` BIGINT,
//			  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECONDS
//          ) WITH (
//				'connector' = 'kafka',
//				'topic' = '${sql.aggregation.playEventsTopic}',
//				'properties.bootstrap.servers' = '${sql.aggregation.kafkaServer}',
//				'key.format' = 'raw',
//				'key.fields' = 'the_kafka_key',
//				'value.format' = 'avro-confluent',
//				'value.avro-confluent.url' = '${sql.aggregation.schemaRegistry}',
//				'value.fields-include' = 'EXCEPT_KEY',
//				'scan.startup.mode' = 'earliest-offset'
//		)

		createKafkaTableDdl(
				toV1alpha1Stream("file:./streaming-runtime/src/test/java/com/vmware/tanzu/streaming/runtime/throwaway/PlayEventsStream.yaml"));

//		CREATE TABLE SongPlays (
//            `song_id` BIGINT NOT NULL,
//            `album` STRING,
//            `artist` STRING,
//            `name` STRING,
//            `genre` STRING NOT NULL,
//            `duration` BIGINT,
//            `event_time` TIMESTAMP(3) NOT NULL,
//		       WATERMARK FOR `event_time` AS `event_time` - INTERVAL '1' SECOND
//          ) WITH (
//				'connector' = 'kafka',
//				'topic' = 'play-events-genre-join',
//				'properties.bootstrap.servers' = '${sql.aggregation.kafkaServer}',
//				'properties.allow.auto.create.topics' = 'true',
//				'properties.group.id' = 'testGroup3',
//				'scan.startup.mode' = 'earliest-offset',
//				'key.format' = 'json',
//				'key.fields' = 'song_id', // TODO
//				'key.json.ignore-parse-errors' = 'true',
//				'value.format' = 'json',
//				'value.json.fail-on-missing-field' = 'false',
//				'value.fields-include' = 'ALL'
//		)

		createKafkaTableDdl(
				toV1alpha1Stream("file:./streaming-runtime/src/test/java/com/vmware/tanzu/streaming/runtime/throwaway/SongPlaysStream.yaml"));

//		CREATE TABLE TopKSongsPerGenre (
//            `window_start` TIMESTAMP(3),
//            `window_end` TIMESTAMP(3),
//            `song_id` BIGINT NOT NULL,
//            `name` STRING NOT NULL,
//            `genre` STRING NOT NULL,
//            `song_play_count` BIGINT,
//			   PRIMARY KEY (`window_start`, `window_end`, `song_id`, `genre`) NOT ENFORCED // TODO
//          ) WITH (
//				'connector' = 'upsert-kafka', // TODO
//				'topic' = '${sql.aggregation.outputSqlAggregateTopic}',
//				'properties.bootstrap.servers' = '${sql.aggregation.kafkaServer}',
//				'properties.allow.auto.create.topics' = 'true', // TODO
//				'key.format' = 'json',
//				'key.json.ignore-parse-errors' = 'true',
//				'value.format' = 'json',
//				'value.json.fail-on-missing-field' = 'false',
//				'value.fields-include' = 'ALL'
//		)

		createKafkaTableDdl(
				toV1alpha1Stream("file:./streaming-runtime/src/test/java/com/vmware/tanzu/streaming/runtime/throwaway/TopKSongsPerGenreStream.yaml"));

	}

	public static V1alpha1Stream toV1alpha1Stream(String streamYamlResourceUri) throws IOException {
		ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
		yamlReader.registerModule(new JavaTimeModule());
		InputStream yaml = new DefaultResourceLoader().getResource(streamYamlResourceUri).getInputStream();
		V1alpha1Stream stream = yamlReader.readValue(yaml, V1alpha1Stream.class);
		return stream;
	}

	public static String toAvroSchema(V1alpha1StreamSpecPayloadSchema payloadSchema) throws JsonProcessingException {
		// Convert the Payload(Meta)Schema into Avro Schema (json).
		ObjectMapper jsonWriter = new ObjectMapper();
		jsonWriter.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		return jsonWriter.writerWithDefaultPrettyPrinter().writeValueAsString(payloadSchema);
	}

	public static String createKafkaTableDdl(V1alpha1Stream stream) throws IOException {

		Map<String, String> attributes = CollectionUtils.isEmpty(stream.getSpec()
				.getAttributes()) ? new HashMap<>() : stream.getSpec().getAttributes();

		String ddlValueFormat = attributes.containsKey("ddlValueFormat") ? attributes.get("ddlValueFormat") : "avro";

		// 1. Convert the Payload(Meta)Schema into Avro Schema (json).
		String avroSchema = toAvroSchema(stream.getSpec().getPayloadSchema());

		//System.out.println(avroSchema);

		// 2. Convert the AvroSchema into DataType
		//    https://github.com/apache/flink/blob/master/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/typeutils/AvroSchemaConverter.java
		org.apache.flink.table.types.DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema);
		//System.out.println(dataType);
		//TypeInformation<Object> ti = AvroSchemaConverter.convertToTypeInfo(avroSchema); System.out.println(ti);

		// 3. Convert the DataType into create-table DDL fields
		List<String> createTableFields = ((RowType) dataType.getLogicalType()).getFields().stream()
				.map(d -> "  `" + d.getName() + "` " + d.getType())
				.collect(Collectors.toList());

		if (StringUtils.hasText(stream.getSpec().getAttributes().get("watermark"))) {
			createTableFields.add("  " + stream.getSpec().getAttributes().get("watermark"));
		}

		if (StringUtils.hasText(stream.getSpec().getAttributes().get("ddlPrimaryKey"))) {
			createTableFields.add("  " + stream.getSpec().getAttributes().get("ddlPrimaryKey"));
		}

		if (ddlValueFormat.contains("avro")) {
			createTableFields.add("  `the_kafka_key` STRING");
		}

		String createTableSection = createTableFields.stream().collect(Collectors.joining(",\n"));

		// 4. Covert the storage address server information into Flink SQL connector WITH section.
		V1alpha1ClusterStreamStatusStorageAddressServers server = stream.getStatus()
				.getStorageAddress().getServers().values().iterator().next();

		List<String> ddlWiths = new ArrayList<>();

		String connectorName = StringUtils.hasText(attributes.get("ddlConnector")) ?
				attributes.get("ddlConnector") : stream.getSpec().getProtocol();
		ddlWiths.add("'connector' = '" + connectorName + "'");
		ddlWiths.add("'topic' = '" + stream.getMetadata().getName() + "'");
		ddlWiths.add("'properties.bootstrap.servers' = '" + server.getVariables().get("brokers") + "'");
		ddlWiths.add("'properties.group.id' = 'testGroup'"); // TODO ???

		if ("avro".equalsIgnoreCase(ddlValueFormat)) { //AVRO
			ddlWiths.add("'key.format' = 'raw'");
			ddlWiths.add("'key.fields' = 'the_kafka_key'");
			ddlWiths.add("'value.fields-include' = 'EXCEPT_KEY'");
			ddlWiths.add("'scan.startup.mode' = 'earliest-offset'");
			if (StringUtils.hasText(server.getVariables().get("schemaRegistry"))) {
				ddlWiths.add("'value.format' = 'avro-confluent'");
				ddlWiths.add("'value.avro-confluent.url' = '" + server.getVariables().get("schemaRegistry") + "'");
			}
			else {
				ddlWiths.add("'value.format' = 'avro'");
			}
		}
		else if ("json".equalsIgnoreCase(ddlValueFormat)) { //JSON
			ddlWiths.add("'key.format' = 'json'");
			ddlWiths.add("'value.format' = 'json'");
			ddlWiths.add("'key.json.ignore-parse-errors' = 'true'");
			ddlWiths.add("'value.json.fail-on-missing-field' = 'false'");
			ddlWiths.add("'value.fields-include' = 'ALL'");
		}

		List<String> ddlWithExtensions = attributes.entrySet().stream()
				.filter(e -> e.getKey().startsWith("ddlWithExtension"))
				.map(e -> e.getValue())
				.collect(Collectors.toList());
		ddlWiths.addAll(ddlWithExtensions);

		String withSection = ddlWiths.stream()
				.map(s -> "  " + s)
				.collect(Collectors.joining(",\n"));

		String createTableDdl = String.format("CREATE TABLE %s (\n%s\n) WITH (\n%s\n)\n",
				stream.getSpec().getPayloadSchema().getName(),
				createTableSection,
				withSection);

		System.out.println(createTableDdl);

		return createTableDdl;
	}
}
