package com.vmware.tanzu.streaming.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamStatusStorageAddressServers;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecPayloadSchema;
import io.kubernetes.client.openapi.ApiException;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

public class ProcessorSqlHelper {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessorReconciler.class);

	private static final Pattern IN_SQL_STREAM_NAME_PATTERN = Pattern.compile("\\[\\[STREAM:(\\S*)\\]\\]", Pattern.CASE_INSENSITIVE);

	public String substituteSqlStreamNames(String sql, Map<String, V1alpha1Stream> map) {
		String outSql = sql;
		for (Map.Entry<String, V1alpha1Stream> e : map.entrySet()) {
			outSql = outSql.replace(e.getKey(), e.getValue().getSpec().getPayloadSchema().getName());
		}
		return outSql;
	}

	public Map<String, String> getInSqlPlaceholderToStreamNameMap(List<String> sqlQueries) throws ApiException {

		Map<String, String> placeholderToStreamMap = new HashMap<>();

		if (!CollectionUtils.isEmpty(sqlQueries)) {
			for (String sql : sqlQueries) {
				Matcher matcher = IN_SQL_STREAM_NAME_PATTERN.matcher(sql);

				for (MatchResult mr : matcher.results().collect(Collectors.toList())) {
					String placeholder = mr.group();
					String streamName = mr.group(1);
					LOG.info(placeholder + " -> " + streamName);
					placeholderToStreamMap.put(placeholder, streamName);
				}
			}
		}
		return placeholderToStreamMap;
	}

	public List<String> getDdlAndSqlStatements(List<String> sqlQueries, Map<String, V1alpha1Stream> inSqlPlaceholderToStreamMap) throws ApiException {
		List<String> executionSqlStatements = new ArrayList<>();

		for (V1alpha1Stream stream : inSqlPlaceholderToStreamMap.values()) {
			String createTableDdl = this.inferFlinkTableDdlFromStream(stream);
			executionSqlStatements.add(createTableDdl);
		}

		if (!CollectionUtils.isEmpty(sqlQueries)) {
			List<String> resolvedSqlQueries = sqlQueries.stream()
					.map(sql -> this.substituteSqlStreamNames(sql, inSqlPlaceholderToStreamMap))
					.collect(Collectors.toList());
			executionSqlStatements.addAll(resolvedSqlQueries);
		}

		LOG.info("" + executionSqlStatements);
		return executionSqlStatements;
	}

	public String inferFlinkTableDdlFromStream(V1alpha1Stream stream) throws ApiException {

		if (!stream.getSpec().getProtocol().contains("kafka")) {
			throw new ApiException("Table DDL inference is only supported for Kafka streams but received:" + stream);
		}
		Map<String, String> attributes = CollectionUtils.isEmpty(stream.getSpec()
				.getAttributes()) ? new HashMap<>() : stream.getSpec().getAttributes();

		String ddlValueFormat = attributes.containsKey("ddlValueFormat") ? attributes.get("ddlValueFormat") : "avro";

		// 1. Convert the Payload(Meta)Schema into Avro Schema (json).
		String avroSchema = null;
		try {
			avroSchema = toAvroSchema(stream.getSpec().getPayloadSchema());
		}
		catch (JsonProcessingException e) {
			LOG.error("Failed to parse Stream's payload meta-schema", e);
			throw new ApiException(e);
		}

		// 2. Convert the AvroSchema into DataType
		//    https://github.com/apache/flink/blob/master/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/typeutils/AvroSchemaConverter.java
		org.apache.flink.table.types.DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema);
		//System.out.println(dataType);
		//TypeInformation<Object> ti = AvroSchemaConverter.convertToTypeInfo(avroSchema); System.out.println(ti);

		// 3. Convert the DataType into create-table DDL fields
		List<String> createTableFields = ((RowType) dataType.getLogicalType()).getFields().stream()
				.map(d -> "  `" + d.getName() + "` " + d.getType())
				.collect(Collectors.toList());

		List<String> additionalColumns = attributes.entrySet().stream()
				.filter(e -> e.getKey().startsWith("ddlColumn"))
				.map(e -> "  " + e.getValue())
				.collect(Collectors.toList());

		createTableFields.addAll(additionalColumns);

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

	private String toAvroSchema(V1alpha1StreamSpecPayloadSchema payloadSchema) throws JsonProcessingException {
		// Convert the Payload(Meta)Schema into Avro Schema (json).
		ObjectMapper jsonWriter = new ObjectMapper();
		jsonWriter.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		return jsonWriter.writerWithDefaultPrettyPrinter().writeValueAsString(payloadSchema);
	}
}
