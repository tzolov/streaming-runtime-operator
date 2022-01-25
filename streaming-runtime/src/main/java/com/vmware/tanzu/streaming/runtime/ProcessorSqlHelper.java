package com.vmware.tanzu.streaming.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamStatusStorageAddressServers;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchema;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaMetadataFields;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaSchema;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaTimeAttributes;
import io.kubernetes.client.openapi.ApiException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.EncodingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

public class ProcessorSqlHelper {
	private static final Logger LOG = LoggerFactory.getLogger(ProcessorReconciler.class);

	private static final Pattern IN_SQL_STREAM_NAME_PATTERN = Pattern.compile("\\[\\[STREAM:(\\S*)\\]\\]", Pattern.CASE_INSENSITIVE);

	public static final String PROCTIME = "proctime";

	private String getSchemaName(V1alpha1StreamSpecDataSchema dataSchema) {
		if (dataSchema.getInline() != null) {
			if (dataSchema.getInline().getType().equalsIgnoreCase("avro")) {
				try {
					return new Schema.Parser().parse(dataSchema.getInline().getSchema()).getName();
				}
				catch (SchemaParseException e) {
					throw new IllegalArgumentException("Could not parse Avro schema string.", e);
				}
			}
		}
		else if (dataSchema.getSchema() != null) {
			return dataSchema.getSchema().getName();
		}

		throw new RuntimeException("Either inline or schema must be provided! None found");

	}

	private String substituteSqlStreamNames(String sql, Map<String, V1alpha1Stream> map) {
		String outSql = sql;
		for (Map.Entry<String, V1alpha1Stream> e : map.entrySet()) {
			outSql = outSql.replace(e.getKey(), this.getSchemaName(e.getValue().getSpec().getDataSchema()));
		}
		return outSql;
	}

	public Map<String, String> getInSqlPlaceholderToStreamNameMap(List<String> sqlQueries) {

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

	private String inferFlinkTableDdlFromStream(V1alpha1Stream stream) throws ApiException {
		V1alpha1StreamSpecDataSchema streamDataSchema = stream.getSpec().getDataSchema();

		// 4. Covert the storage address server information into Flink SQL connector WITH section.
		V1alpha1ClusterStreamStatusStorageAddressServers server = stream.getStatus()
				.getStorageAddress().getServers().values().iterator().next();

		String protocol = stream.getSpec().getProtocol();
		String streamMetadataName = stream.getMetadata().getName();
		Map<String, String> serverVariables = server.getVariables();

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

		return toDdlInternal(streamDataSchema, allOptions);
	}

	public String toDdlInternal(V1alpha1StreamSpecDataSchema streamDataSchema, Map<String, String> allOptions) throws ApiException {

		String dataSchemaName = getSchemaName(streamDataSchema);

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
		String avroSchema = null;
		if (streamDataSchema.getInline() != null) {
			if (streamDataSchema.getInline().getType().equalsIgnoreCase("avro")) {
				avroSchema = streamDataSchema.getInline().getSchema();
			}
		}
		else if (streamDataSchema.getSchema() != null) {
			avroSchema = toAvroSchema(streamDataSchema.getSchema(), metadataFields, timeAttributeMap, timeAttributeFieldMap);
		}
		else {
			throw new ApiException("Either inline or schema must be provided! None found");
		}

		System.out.println(avroSchema);

		// 2. Convert the AVRO schema into Flink DataType
		org.apache.flink.table.types.DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema);

		Map<String, String> justAddedColumns = new ConcurrentHashMap<>();

		org.apache.flink.table.api.Schema.Builder flinkSchemaBuilder =
				org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(dataType);

		// Options
		Map<String, String> tableOptions = allOptions.entrySet().stream()
				.filter(e -> e.getKey().startsWith("ddl."))
				.collect(Collectors.toMap(e -> e.getKey().substring("ddl.".length()), Map.Entry::getValue));

		// Add column for the raw key.fields
		if (tableOptions.containsKey("key.format") && "raw".equalsIgnoreCase(tableOptions.get("key.format"))
				&& tableOptions.containsKey("key.fields")) {
			String rawKeyFormatColumnName = tableOptions.get("key.fields");
			flinkSchemaBuilder.column(rawKeyFormatColumnName, DataTypes.STRING().notNull());
			justAddedColumns.put(rawKeyFormatColumnName, "");
		}

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
			justAddedColumns.put(metadataField.getName(), "");
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
					justAddedColumns.put(field.getName(), "");
				}
			}
		}
		else {
			for (String timeAttributeKey : timeAttributeMap.keySet()) {
				Optional<RowType.RowField> row = ((RowType) dataType.getLogicalType()).getFields().stream()
						.filter(f -> f.getName().equalsIgnoreCase(timeAttributeKey)).findFirst();

				boolean isRowExist = row.isPresent() || justAddedColumns.containsKey(timeAttributeKey);
				String watermark = timeAttributeMap.get(timeAttributeKey);
				if (isRowExist) {
					if (StringUtils.hasText(watermark)) {
						// Event Time
						flinkSchemaBuilder.watermark(timeAttributeKey, new SqlCallExpression(watermark));
					}
					else {
						// TODO check that the existing column already has a PROCTIME time
					}
				}
				else {
					if (!StringUtils.hasText(watermark)) {
						// PROC TIME
						flinkSchemaBuilder.columnByExpression(timeAttributeKey, new SqlCallExpression("PROCTIME()"));
						justAddedColumns.put(timeAttributeKey, "");
					}
					else {
						throw new RuntimeException(
								String.format("Missing table [%s] column for Event Time attribute: %s, %s",
										dataSchemaName, timeAttributeKey, watermark));
					}
				}
			}
		}

		org.apache.flink.table.api.Schema flinkSchema = flinkSchemaBuilder.build();

		return new SqlPrinter().toDdl(dataSchemaName, flinkSchema, tableOptions);
	}

	private String toAvroSchema(V1alpha1StreamSpecDataSchemaSchema streamSchema,
			Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> metadataFields,
			Map<String, String> timeAttributeMap,
			Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> timeAttributeFieldMap) {

		List<V1alpha1StreamSpecDataSchemaMetadataFields> schemaFields = streamSchema.getFields();

		SchemaBuilder.FieldAssembler<Schema> recordFieldBuilder = SchemaBuilder
				.record(streamSchema.getName())
				.namespace(streamSchema.getNamespace())
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

			if (!metadataFields.containsKey(field.getName()) && !isProcTimeAttribute(field)) { // Ignores the metadata attributes and the time attributes from the Avro schema

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

		return avroSchema.toString(true);
	}

	private DataType toDataType(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		Schema fieldTypeSchema = Schema.create(Schema.Type.valueOf(field.getType().toUpperCase()));
		if (StringUtils.hasText(field.getLogicalType())) {
			fieldTypeSchema = new LogicalType(field.getLogicalType()).addToSchema(fieldTypeSchema); // add logical type
		}
		if (field.getOptional() != null && field.getOptional()) {
			fieldTypeSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldTypeSchema); // add ["null", "type"] union
		}
		return AvroSchemaConverter.convertToDataType(fieldTypeSchema.toString(false));
	}

	private void normalizeFieldType(V1alpha1StreamSpecDataSchemaMetadataFields field) {

		// Normalize the field's type and logicalType fields
		if (field.getType().split("_").length > 1) {
			String[] typeAndLogicalType = field.getType().split("_");
			field.setLogicalType(typeAndLogicalType[1]);
			field.setType(typeAndLogicalType[0]);
		}
	}

	private boolean isTimeAttribute(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		return isProcTimeAttribute(field) || StringUtils.hasText(field.getWatermark());
	}

	private boolean isProcTimeAttribute(V1alpha1StreamSpecDataSchemaMetadataFields field) {
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