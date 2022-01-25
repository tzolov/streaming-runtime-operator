package com.vmware.tanzu.streaming.runtime.throwaway;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchema;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaMetadataFields;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaTimeAttributes;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;

import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

public class AvroSchemaBuilding2 {


	public static final String PROCTIME = "proctime";

	public static void main(String[] args) throws JsonProcessingException {
		experiment1_1();
	}

	public static void experiment1_1() throws JsonProcessingException {
		String inYaml = "--- \n"
				+ "schema: \n"
				+ "  namespace: net.tzolov.poc.playsongs.avro\n"
				+ "  name: SongPlays\n"
				+ "  fields: \n"
				+ "    - name: song_id\n"
				+ "      type: long\n"
				+ "    - name: album\n"
				+ "      optional: true\n"
				+ "      type: string\n"
				+ "    - name: artist\n"
				+ "      optional: true\n"
				+ "      type: string\n"
				+ "    - name: name\n"
				+ "      optional: true\n"
				+ "      type: string\n"
				+ "    - name: genre\n"
				+ "      type: string\n"
				+ "    - name: duration\n"
				+ "      optional: true\n"
				+ "      type: long\n"
				+ "    - name: event_time\n"
				+ "      type: long_timestamp-millis\n"
//				+ "      type: long\n"
//				+ "      logicalType: timestamp-millis\n"
				+ "    - name: timestamp2\n"
				+ "      type: long_timestamp-millis\n"
				+ "    - name: event_time3\n"
				+ "      type: long_timestamp-millis\n"
				+ "      metadata:\n"
				+ "        from: event_time3\n"
				+ "        readonly: true\n"
				+ "    - name: event_time4\n"
				+ "      type: proctime\n"
				+ "    - name: event_time5\n"
				+ "      type: long_timestamp-millis\n"
				+ "      watermark: \"`event_time5` - INTERVAL '6' SECONDS\"\n"
				+ "metadataFields:\n"
				+ "  - name: timestamp2\n"
				+ "    type: long_timestamp-millis\n"
				+ "    metadata:\n"
				+ "      from: timestamp\n"
				+ "primaryKey: [ song_id, name ]\n"
				+ "timeAttributes: \n"
				+ "  - name: event_time\n"
//				+ "    watermark: \"`event_time` - INTERVAL '1' SECOND\"\n"
				+ "options: \n"
				+ "  ddl.key.fields: song_id\n"
				+ "  ddl.properties.allow.auto.create.topics: \"true\"\n"
				+ "  ddl.properties.group.id: testGroup3\n"
				+ "  ddl.scan.startup.mode: earliest-offset\n"
				+ "  ddl.value.format: json\n";


		ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
		yamlReader.registerModule(new JavaTimeModule());

		V1alpha1StreamSpecDataSchema streamDataSchema = yamlReader.readValue(inYaml, V1alpha1StreamSpecDataSchema.class);

		String protocol = "kafka";
		String streamMetadataName = "myTestStream";
		Map<String, String> serverVariables = new HashMap<>();
		serverVariables.put("brokers", "http:localhost");

		Map<String, String> customOptions = new HashMap<>();
		Map<String, String> rawOptions = streamDataSchema.getOptions();

		String ddlValueFormat = rawOptions.containsKey("ddl.value.format") ? rawOptions.get("ddl.value.format") : "avro";
		customOptions.put("ddl.value.format", ddlValueFormat);

		String connector = rawOptions.containsKey("ddl.connector") ? rawOptions.get("ddl.connector") : protocol;
		customOptions.put("ddl.connector", connector);
		customOptions.put("ddl.topic", streamMetadataName);
		customOptions.put("ddl.properties.bootstrap.servers", serverVariables.get("brokers"));

		if ("avro".equalsIgnoreCase(ddlValueFormat)) { //AVRO
			customOptions.put("ddl.key.format", "raw");
			customOptions.put("ddl.key.fields", "the_kafka_key");
			customOptions.put("ddl.value.fields-include", "EXCEPT_KEY");
			// ddlWiths.add("'scan.startup.mode' = 'earliest-offset'"); // TODO explicit
			if (StringUtils.hasText(serverVariables.get("schemaRegistry"))) {
				customOptions.put("ddl.value.format", "avro-confluent");
				customOptions.put("ddl.value.avro-confluent.url", serverVariables.get("schemaRegistry"));
			}
		}
		else if ("json".equalsIgnoreCase(ddlValueFormat)) { //JSON
			customOptions.put("ddl.key.format", "json");
			customOptions.put("ddl.value.format", "json");
			customOptions.put("ddl.key.json.ignore-parse-errors", "true");
			customOptions.put("ddl.value.json.fail-on-missing-field", "false");
			customOptions.put("ddl.value.fields-include", "ALL");
		}

		Map<String, String> allOptions = new HashMap<>(rawOptions);
		allOptions.putAll(customOptions);

		System.out.println("-------------------------------------------");
		System.out.println(toDdlInternal(streamDataSchema, allOptions));
		System.out.println("-------------------------------------------");

	}

	public static String toDdlInternal(V1alpha1StreamSpecDataSchema streamDataSchema, Map<String, String> allOptions ) {

		String schemaName = streamDataSchema.getSchema().getName();
		String schemaNamespace = streamDataSchema.getSchema().getNamespace();

//		// Options
//		Map<String, String> allOptions = streamDataSchema.getOptions();
		Map<String, String> tableOptions = allOptions.entrySet().stream()
				.filter(e -> e.getKey().startsWith("ddl."))
				.collect(Collectors.toMap(e -> e.getKey().substring("ddl.".length()), Map.Entry::getValue));


		// Extract metadata attributes as map by field names.
		List<V1alpha1StreamSpecDataSchemaMetadataFields> metaAttributes = streamDataSchema.getMetadataFields();
		if (metaAttributes == null) {
			metaAttributes = new ArrayList<>();
		}
		Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> metadataFields = metaAttributes.stream()
				.collect(Collectors.toMap(V1alpha1StreamSpecDataSchemaMetadataFields::getName, f -> f));


		// Retrieve the time attributes
		List<V1alpha1StreamSpecDataSchemaTimeAttributes> timeAttributes = streamDataSchema.getTimeAttributes();
		if (timeAttributes == null) {
			timeAttributes = new ArrayList<>();
		}
		Map<String, String> timeAttributeMap = timeAttributes.stream()
				.collect(Collectors.toMap(
						V1alpha1StreamSpecDataSchemaTimeAttributes::getName,
						ta -> (ta.getWatermark() == null) ? "" : ta.getWatermark()));
		Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> timeAttributeFieldMap = new HashMap<>();


		// 1. Create AVRO Schema from the CR's built-in dataSchema
		List<V1alpha1StreamSpecDataSchemaMetadataFields> schemaFields = streamDataSchema.getSchema().getFields();

		SchemaBuilder.FieldAssembler<Schema> recordFieldBuilder = SchemaBuilder
				.record(schemaName)
				.namespace(schemaNamespace)
				.fields();

		for (V1alpha1StreamSpecDataSchemaMetadataFields field : schemaFields) {

			// Normalize the field's type and logicalType fields
			normalizeFieldType(field);

			if (isTimeAttribute(field)) {
				timeAttributeFieldMap.put(field.getName(), field);
				if (!timeAttributeMap.containsKey(field.getName())) {
					timeAttributeMap.put(field.getName(), field.getWatermark());
				}
			}
			if (timeAttributeMap.containsKey(field.getName())) {
				if (!timeAttributeFieldMap.containsKey(field.getName())) {
					timeAttributeFieldMap.put(field.getName(), field);
				}
				field.setWatermark(timeAttributeMap.get(field.getName())); // Always use the outer watermark definition if present! E.g. outer definition overrides the in-field time-attributes.
				if (!StringUtils.hasText(field.getWatermark())) { // time attribute without watermark defaults to PROCTIME
					field.setType(PROCTIME);
				}
			}

			if (field.getMetadata() != null) {
				metadataFields.put(field.getName(), field);
			}

			if (!metadataFields.containsKey(field.getName()) && !isProcTimeAttribute(field)) { // Skip the metadata and the time attributes from the Avro schema

				Schema fieldTypeSchema = Schema.create(Schema.Type.valueOf(field.getType().toUpperCase()));

				if (StringUtils.hasText(field.getLogicalType())) {
					fieldTypeSchema = new LogicalType(field.getLogicalType()).addToSchema(fieldTypeSchema); // add logical type
				}

				Boolean optional = field.getOptional();
				if (optional != null && optional) {
					fieldTypeSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldTypeSchema); // add ["null", "type"] union
				}

				recordFieldBuilder.name(field.getName()).type(fieldTypeSchema).noDefault();
			}

		}
		Schema avroSchema = recordFieldBuilder.endRecord();

		String avroSchemaString = avroSchema.toString(true);
		System.out.println(avroSchemaString);

		// 2. Convert the AVRO schema into Flink DataType
		org.apache.flink.table.types.DataType dataType = AvroSchemaConverter.convertToDataType(avroSchemaString);

		org.apache.flink.table.api.Schema.Builder flinkSchemaBuilder =
				org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(dataType);

		// METADATA FIELDS

		for (V1alpha1StreamSpecDataSchemaMetadataFields metadataField : metadataFields.values()) {
			normalizeFieldType(metadataField);
			DataType fieldDataType = toDataType(metadataField);
			flinkSchemaBuilder.columnByMetadata(
					metadataField.getName(),
					fieldDataType,
					(metadataField.getName().equalsIgnoreCase(metadataField.getMetadata()
							.getFrom())) ? null : metadataField.getMetadata().getFrom(),
					Optional.ofNullable(metadataField.getMetadata().getReadonly()).orElse(false));
		}

		// PRIMARY KEY
		List<String> primaryKey = streamDataSchema.getPrimaryKey();
		if (!CollectionUtils.isEmpty(primaryKey)) {
			flinkSchemaBuilder.primaryKey(primaryKey);
		}

		// TIME ATTRIBUTES
		if (!CollectionUtils.isEmpty(timeAttributeFieldMap)) {
			for (V1alpha1StreamSpecDataSchemaMetadataFields field : timeAttributeFieldMap.values()) {
				if (!isProcTimeAttribute(field)) {
					// Event Time
					flinkSchemaBuilder.watermark(field.getName(), new SqlCallExpression(field.getWatermark()));
				}
				else {
					// Processing Time TODO PROCTIME MUST ALWAYS BE the LAST COLUMN !!!!
					flinkSchemaBuilder.columnByExpression(field.getName(), new SqlCallExpression("PROCTIME()"));
				}
			}
		}

		org.apache.flink.table.api.Schema flinkSchema = flinkSchemaBuilder.build();


		return new SqlPrinter().toDdl(schemaName, flinkSchema, tableOptions);
	}

	public static DataType toDataType(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		Schema fieldTypeSchema = Schema.create(Schema.Type.valueOf(field.getType().toUpperCase()));
		if (StringUtils.hasText(field.getLogicalType())) {
			fieldTypeSchema = new LogicalType(field.getLogicalType()).addToSchema(fieldTypeSchema); // add logical type
		}
		if (field.getOptional() != null && field.getOptional()) {
			fieldTypeSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldTypeSchema); // add ["null", "type"] union
		}
		return AvroSchemaConverter.convertToDataType(fieldTypeSchema.toString(false));
	}

	public static void normalizeFieldType(V1alpha1StreamSpecDataSchemaMetadataFields field) {

		// Normalize the field's type and logicalType fields
		if (field.getType().split("_").length > 1) {
			String[] typeAndLogicalType = field.getType().split("_");
			field.setLogicalType(typeAndLogicalType[1]);
			field.setType(typeAndLogicalType[0]);
		}
	}

	public static boolean isTimeAttribute(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		return isProcTimeAttribute(field) || StringUtils.hasText(field.getWatermark());
	}

	public static boolean isProcTimeAttribute(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		return PROCTIME.equalsIgnoreCase(field.getType());
	}

	public static class SqlPrinter {

		public String toDdl(String schemaName, org.apache.flink.table.api.Schema flinkSchema, Map<String, String> tableOptions) {
			List<String> allColumns = new ArrayList<>();

			allColumns.addAll(flinkSchema.getColumns().stream().map(this::printColumn)
					.collect(Collectors.toList()));
			allColumns.addAll(flinkSchema.getWatermarkSpecs().stream().map(this::printWatermark)
					.collect(Collectors.toList()));
			flinkSchema.getPrimaryKey().ifPresent(pk -> allColumns.add(this.printPrimaryKey(pk)));


			return String.format("CREATE TABLE %s (%n%s%n) WITH (%n%s %n)%n", schemaName,
					allColumns.stream().map(s -> "   " + s).collect(Collectors.joining(",\n")),
					this.printOptions(tableOptions));
		}

		private String printWatermark(org.apache.flink.table.api.Schema.UnresolvedWatermarkSpec watermarkSpec) {
			String expression = watermarkSpec.getWatermarkExpression().asSummaryString();
			return String.format(
					"WATERMARK FOR %s AS %s",
					EncodingUtils.escapeIdentifier(watermarkSpec.getColumnName()),
					expression.substring(1, expression.length() - 1));

		}

		private String printColumn(org.apache.flink.table.api.Schema.UnresolvedColumn column) {
			if (column instanceof org.apache.flink.table.api.Schema.UnresolvedMetadataColumn) {
				return printColumn((org.apache.flink.table.api.Schema.UnresolvedMetadataColumn) column);
			}
			else if (column instanceof org.apache.flink.table.api.Schema.UnresolvedComputedColumn) {
				return printColumn((org.apache.flink.table.api.Schema.UnresolvedComputedColumn) column);
			}
			else if (column instanceof org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn) {
				return printColumn((org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn) column);
			}
			else {
				final StringBuilder sb = new StringBuilder();
				sb.append(EncodingUtils.escapeIdentifier(column.getName()));
				return sb.toString();
			}
		}

		private String printColumn(org.apache.flink.table.api.Schema.UnresolvedMetadataColumn column) {
			final StringBuilder sb = new StringBuilder();
			sb.append(EncodingUtils.escapeIdentifier(column.getName()));
			sb.append(column.getDataType().toString());
			sb.append(" METADATA");
			if (column.getMetadataKey() != null) {
				sb.append(" FROM '");
				sb.append(EncodingUtils.escapeSingleQuotes(column.getMetadataKey()));
				sb.append("'");
			}
			if (column.isVirtual()) {
				sb.append(" VIRTUAL");
			}
			column.getComment().ifPresent(
					c -> {
						sb.append(" COMMENT '");
						sb.append(EncodingUtils.escapeSingleQuotes(c));
						sb.append("'");
					});
			return sb.toString();
		}

		private String printColumn(org.apache.flink.table.api.Schema.UnresolvedComputedColumn column) {
			String expressionStr = column.getExpression().asSummaryString();
			final StringBuilder sb = new StringBuilder();
			sb.append(String.format("%s AS %s", EncodingUtils.escapeIdentifier(column.getName()), expressionStr.substring(1, expressionStr.length() - 1))); // Removes the outer [...] brackets
			column.getComment().ifPresent(
					c -> {
						sb.append(" COMMENT '");
						sb.append(EncodingUtils.escapeSingleQuotes(c));
						sb.append("'");
					});
			return sb.toString();
		}

		private String printColumn(org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn column) {
			final StringBuilder sb = new StringBuilder();
			sb.append(String.format("%s %s", EncodingUtils.escapeIdentifier(column.getName()), column.getDataType()
					.toString()));
			column.getComment().ifPresent(
					c -> {
						sb.append(" COMMENT '");
						sb.append(EncodingUtils.escapeSingleQuotes(c));
						sb.append("'");
					});
			return sb.toString();
		}

		private String printPrimaryKey(org.apache.flink.table.api.Schema.UnresolvedPrimaryKey primaryKey) {

			return String.format(
					"PRIMARY KEY (%s) NOT ENFORCED",
					primaryKey.getColumnNames().stream()
							.map(EncodingUtils::escapeIdentifier)
							.collect(Collectors.joining(", ")));

		}

		private String printOptions(Map<String, String> options) {
			return options.entrySet().stream()
					.map(entry -> String.format("  '%s' = '%s'",
							EncodingUtils.escapeSingleQuotes(entry.getKey()),
							EncodingUtils.escapeSingleQuotes(entry.getValue())))
					.collect(Collectors.joining(String.format(",%n")));
		}
	}
}
