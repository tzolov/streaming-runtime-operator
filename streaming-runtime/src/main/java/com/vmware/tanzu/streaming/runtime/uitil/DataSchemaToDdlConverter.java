package com.vmware.tanzu.streaming.runtime.uitil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

@Component
public class DataSchemaToDdlConverter {

	private static final Logger LOG = LoggerFactory.getLogger(DataSchemaToDdlConverter.class);

	public static final String PROCTIME = "proctime";
	public static final String INLINE_SCHEMA_TYPE_AVRO_CONFLUENT = "avro-confluent";
	public static final String INLINE_SCHEMA_TYPE_AVRO = "avro";
	public static final String INLINE_SCHEMA_TYPE_SQL = "sql";
	public static final String OPTIONS_PREFIX = "ddl.";

	private final SimpleSqlToAvroSchemaConverter sqlToAvroSchemaConverter;
	private final FlinkSchemaToSqlWriter flinkSchemaToSqlConvertor;

	public DataSchemaToDdlConverter(SimpleSqlToAvroSchemaConverter sqlToAvroSchemaConverter,
			FlinkSchemaToSqlWriter flinkSchemaToSqlConvertor) {
		this.sqlToAvroSchemaConverter = sqlToAvroSchemaConverter;
		this.flinkSchemaToSqlConvertor = flinkSchemaToSqlConvertor;
	}

	/**
	 * Holder of the Table Name and CREATE TABLE DDL.
	 */
	public static class TableDdlInfo {
		private final String tableName;
		private final String tableDdl;

		public TableDdlInfo(String tableName, String tableDdl) {
			this.tableDdl = tableDdl;
			this.tableName = tableName;
		}

		public String getTableDdl() {
			return tableDdl;
		}

		public String getTableName() {
			return tableName;
		}
	}

	/**
	 * Convert a Stream data-schema into executable Flink CREATE TABLE DDL with related time-attributes, metadata and
	 * options.
	 * @param stream Single stream CD with data schema for which DDL is generated.
	 * @return Returns Stream data-schema into executable Flink CREATE TABLE DDL with related time-attributes, metadata and options.
	 */
	public TableDdlInfo createTableDdl(V1alpha1Stream stream) throws ApiException {

		V1alpha1StreamSpecDataSchema streamDataSchema = stream.getSpec().getDataSchema();

		// Covert the storage address server information into Flink SQL connector WITH section.
		V1alpha1ClusterStreamStatusStorageAddressServers server = stream.getStatus()
				.getStorageAddress().getServers().values().iterator().next();

		String streamProtocol = stream.getSpec().getProtocol();
		String streamMetadataName = stream.getMetadata().getName();
		Map<String, String> serverVariables = server.getVariables();

		Map<String, String> customOptions = new HashMap<>();
		Map<String, String> rawOptions = Optional.ofNullable(streamDataSchema.getOptions()).orElse(new HashMap<>());

		// If the value format is not set, default to avro.
		String ddlValueFormat = Optional.ofNullable(rawOptions.get("ddl.value.format")).orElse("avro");
		customOptions.put("ddl.value.format", ddlValueFormat);

		String connector = Optional.ofNullable(rawOptions.get("ddl.connector")).orElse(streamProtocol);
		customOptions.put("ddl.connector", connector);
		customOptions.put("ddl.topic", streamMetadataName);
		customOptions.put("ddl.properties.bootstrap.servers", serverVariables.get("brokers"));

		// Add the following, additional, key and value configurations in case of Avro or Json value formats
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

		return createFlinkTableDdl(streamDataSchema, allOptions);
	}

	/**
	 * Converts the dataSchema and related options into executable Flink Create Table DDL statement.
	 * @param streamDataSchema Input stream data schema to convert into Flink DDL.
	 * @param options Input schema options to apply to for the executable schema.
	 * @return The TableDdlInfo contains the CREATE TABLE DDL statement and the related Table name.
	 *
	 * @throws ApiException
	 */
	private TableDdlInfo createFlinkTableDdl(
			V1alpha1StreamSpecDataSchema streamDataSchema, Map<String, String> options) throws ApiException {

		if (streamDataSchema == null) {
			throw new ApiException("Missing dataSchema");
		}

		// Retrieve Stream's metadata fields grouped by field names.
		Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> metadataFields =
				Optional.ofNullable(streamDataSchema.getMetadataFields()).orElse(new ArrayList<>()).stream()
						.collect(Collectors.toMap(V1alpha1StreamSpecDataSchemaMetadataFields::getName, f -> f));

		// Retrieve Stream's time attributes in pairs of field-name -> watermark-value.
		// Empty watermark stands Proctime time attribute!
		Map<String, String> timeAttributes = Optional.ofNullable(streamDataSchema.getTimeAttributes())
				.orElse(new ArrayList<>()).stream()
				.collect(Collectors.toMap(
						V1alpha1StreamSpecDataSchemaTimeAttributes::getName,
						ta -> Optional.ofNullable(ta.getWatermark()).orElse("")));

		// Create AVRO Schema from the CR's data schema (inline or built-in)
		String avroSchema = dataSchemaToAvro(streamDataSchema, metadataFields, timeAttributes);

		// Extract the schema name
		String avroSchemaName = new org.apache.avro.Schema.Parser().parse(avroSchema).getName();

		// Turn the Avro schema into Flink DataType
		org.apache.flink.table.types.DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema);

		// Helper class that allow to check if a column name is already present in the target table.
		// The column can either exist as part of the initial avro/datatype schema or added as time/metadata attribute.
		ColumnExistenceChecker columnExistenceChecker = new ColumnExistenceChecker(dataType);

		// BEGIN FLINK TABLE BUILDING
		org.apache.flink.table.api.Schema.Builder flinkSchemaBuilder =
				org.apache.flink.table.api.Schema.newBuilder().fromRowDataType(dataType);

		// Retrieve schema's options and remove the prefix (e.g. make them Flink align).
		Map<String, String> tableOptions = options.entrySet().stream()
				.filter(e -> e.getKey().startsWith(OPTIONS_PREFIX))
				.collect(Collectors.toMap(e -> e.getKey().substring(OPTIONS_PREFIX.length()), Map.Entry::getValue));

		// Add column for the raw key.fields.
		// TODO this is an opinionated assumption that key.format = "raw" and not empty "key.fields" would have
		//      to add those fields to the table if not present.
		if ("raw".equalsIgnoreCase(tableOptions.get("key.format")) && tableOptions.containsKey("key.fields")) {
			String rawKeyFormatColumnNames = tableOptions.get("key.fields");
			for (String columnName : rawKeyFormatColumnNames.split(",")) {
				if (!columnExistenceChecker.isColumnExist(columnName)) {
					flinkSchemaBuilder.column(columnName, DataTypes.STRING().notNull());
					columnExistenceChecker.markColumnNameAsExisting(columnName);
				}
			}
		}

		// METADATA FIELDS: Add the explicitly defined metadata fields as table columns.
		for (V1alpha1StreamSpecDataSchemaMetadataFields metadataField : metadataFields.values()) {

			normalizeFieldType(metadataField);

			String metadataFrom = metadataField.getMetadata().getFrom();

			if (metadataField.getName().equalsIgnoreCase(metadataField.getMetadata().getFrom())) {
				metadataFrom = null; // if both the field name and the metadataFrom name are the same skip the second.
			}

			flinkSchemaBuilder.columnByMetadata(
					metadataField.getName(),
					dataTypeOf(metadataField),
					metadataFrom,
					Optional.ofNullable(metadataField.getMetadata().getReadonly()).orElse(false));

			columnExistenceChecker.markColumnNameAsExisting(metadataField.getName());
		}

		// PRIMARY KEY: If defined convert the primary key into table primary-key constrain.
		List<String> primaryKey = streamDataSchema.getPrimaryKey();
		if (!CollectionUtils.isEmpty(primaryKey)) {
			flinkSchemaBuilder.primaryKey(primaryKey);
		}

		// TIME ATTRIBUTES: Convert the collected time attributes into WATERMARKS or PROCTIME columns.
		if (!CollectionUtils.isEmpty(timeAttributes)) {
			for (String timeAttributeFieldName : timeAttributes.keySet()) {

				// Not-Empty watermark stands for EVENT-TIME time attribute.
				// Empty watermark stands for PROCTIME time attribute.
				String watermark = timeAttributes.get(timeAttributeFieldName);

				if (columnExistenceChecker.isColumnExist(timeAttributeFieldName)) {
					if (StringUtils.hasText(watermark)) {// Event Time
						flinkSchemaBuilder.watermark(timeAttributeFieldName, new SqlCallExpression(watermark));
					}
					else {
						// TODO check that the existing column already has a PROCTIME time
					}
				}
				else { // add new column
					if (!StringUtils.hasText(watermark)) { // PROC TIME
						flinkSchemaBuilder.columnByExpression(timeAttributeFieldName, new SqlCallExpression("PROCTIME()"));
					}
					else {
						throw new RuntimeException(
								String.format("Missing table [%s] column for Event Time attribute: %s, %s",
										avroSchemaName, timeAttributeFieldName, watermark));
					}
				}
			}
		}

		// END FLINK TABLE BUILDING
		org.apache.flink.table.api.Schema flinkSchema = flinkSchemaBuilder.build();

		return new TableDdlInfo(avroSchemaName,
				this.flinkSchemaToSqlConvertor.toSql(avroSchemaName, flinkSchema, tableOptions));
	}

	/**
	 * Covert the Stream data-schema in any of the supported formats (meta-schema, inline-avro and inline-sql)
	 * into Avro schema.
	 * @param streamDataSchema Stream's data-schema definition.
	 * @param metadataFields metadata fields defined explicitly in the Stream's data-schema outer section.
	 * @param timeAttributes time attributes defined explicitly in the Stream's data-schema outer section.
	 * @return Returns Avro schema that represents the data-schema.
	 * @throws ApiException
	 */
	private String dataSchemaToAvro(V1alpha1StreamSpecDataSchema streamDataSchema,
			Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> metadataFields,
			Map<String, String> timeAttributes) throws ApiException {

		if (streamDataSchema.getInline() != null) {
			if (streamDataSchema.getInline().getType().equalsIgnoreCase(INLINE_SCHEMA_TYPE_AVRO)) {
				return streamDataSchema.getInline().getSchema();
			}
			else if (streamDataSchema.getInline().getType().equalsIgnoreCase(INLINE_SCHEMA_TYPE_AVRO_CONFLUENT)) {
				RestTemplate restTemplate = new RestTemplateBuilder().build();
				String schemaUrl = streamDataSchema.getInline().getSchema();
				Map<String, ?> subject = restTemplate.getForObject(schemaUrl, Map.class);
				String avroSchema = (String) subject.get("schema");
				LOG.info("Schema registry schema:" + avroSchema);
				return avroSchema;
			}
			else if (streamDataSchema.getInline().getType().equalsIgnoreCase(INLINE_SCHEMA_TYPE_SQL)) {
				return this.sqlToAvroSchemaConverter.parse(streamDataSchema.getInline().getSchema())
						.toString(true);
			}
			else {
				throw new ApiException(String.format("Unknown inline data-schema type: %s for %s",
						streamDataSchema.getInline().getType(), streamDataSchema));
			}
		}
		else if (streamDataSchema.getSchema() != null) {
			return metaSchemaToAvroSchema(streamDataSchema.getSchema(), metadataFields, timeAttributes);
		}

		throw new ApiException("Either inline or schema expected but none found");
	}

	/**
	 * Converts the Meta-Schema defined in the Stream CRD into Avro Schema.
	 * 	The meta-schema time attributes and metadata fields are stripped from the Avro schema and added to the
	 * 	metadataFields and timeAttributes instead.
	 *
	 * @param streamSchema Stream CRD data meta-schema.
	 * @param metadataFields Metadata fields, explicitly defined in the Stream CD.
	 * @param timeAttributes Time attributes, explicitly defined in the Stream CD.
	 * @return Returns Avro Schema that represents the Meta-Schema.
	 *         The meta-schema time attributes and metadata fields are stripped from the Avro schema and added to the
	 *         metadataFields and timeAttributes instead.
	 */
	private String metaSchemaToAvroSchema(V1alpha1StreamSpecDataSchemaSchema streamSchema,
			Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> metadataFields,
			Map<String, String> timeAttributes) {

		// Start Avro Schema Builder.
		SchemaBuilder.FieldAssembler<Schema> recordFieldBuilder = SchemaBuilder.record(streamSchema.getName())
				.namespace(streamSchema.getNamespace()).fields();

		for (V1alpha1StreamSpecDataSchemaMetadataFields field : streamSchema.getFields()) {
			// Normalize the field's type and logicalType fields
			normalizeFieldType(field);

			// If a time-attribute field that is not defined already in the data-schema's outer time-attributes
			// section add it map of time attributes.
			if (isTimeAttribute(field) && !timeAttributes.containsKey(field.getName())) {
				timeAttributes.put(field.getName(), field.getWatermark()); // the field should not override the outer time attribute!
			}

			// if a metadata field that is not defined in the CD's outer metadata section add the field to the list
			// of metadata fields.
			// The meta-schema field should not override the outer metadata attribute!
			if (field.getMetadata() != null && !metadataFields.containsKey(field.getName())) {
				metadataFields.put(field.getName(), field);
			}

			// Unless a metadata or proctime field add it to the target Avro schema.
			// Note that by design the resolved data-schema should contain only data related fields. The metadata mapping
			// or time-attribute related information is managed by final schema resolved for particular
			// target aggregator such as Flink, KSQL, ...
			if (!metadataFields.containsKey(field.getName()) && !isProcTimeAttribute(field)) {
				recordFieldBuilder
						.name(field.getName())
						.type(fieldToAvroType(field))
						.noDefault();
			}
		}
		Schema avroSchema = recordFieldBuilder.endRecord();
		return avroSchema.toString(true);
	}

	/**
	 * The data MetaSchema allow defining time-attributes to the fields in two ways:
	 *  1. Set the field type to 'proctime' indicating Proctime time-attribute.
	 *  2. Add watermark attribute to the field indicating Event-Time time attribute.
	 */
	private boolean isTimeAttribute(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		return isProcTimeAttribute(field) || StringUtils.hasText(field.getWatermark());
	}

	private boolean isProcTimeAttribute(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		return PROCTIME.equalsIgnoreCase(field.getType());
	}

	private Schema fieldToAvroType(V1alpha1StreamSpecDataSchemaMetadataFields field) {

		normalizeFieldType(field);

		Schema avroType = Schema.create(Schema.Type.valueOf(field.getType().toUpperCase()));

		if (StringUtils.hasText(field.getLogicalType())) {
			// add logical type
			avroType = new LogicalType(field.getLogicalType()).addToSchema(avroType);
		}

		if (Optional.ofNullable(field.getOptional()).orElse(false)) {
			// add ["null", "type"] union
			avroType = nullableSchema(avroType);
		}

		return avroType;
	}

	private Schema nullableSchema(Schema schema) {
		return schema.isNullable()
				? schema
				: Schema.createUnion(SchemaBuilder.builder().nullType(), schema);
	}

	/**
	 * Converts field's type int Flink DataType.
	 */
	private DataType dataTypeOf(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		Schema fieldTypeSchema = Schema.create(Schema.Type.valueOf(field.getType().toUpperCase()));
		if (StringUtils.hasText(field.getLogicalType())) {
			fieldTypeSchema = new LogicalType(field.getLogicalType()).addToSchema(fieldTypeSchema); // add logical type
		}
		if (field.getOptional() != null && field.getOptional()) {
			fieldTypeSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldTypeSchema); // add ["null", "type"] union
		}
		return AvroSchemaConverter.convertToDataType(fieldTypeSchema.toString(false));
	}

	/**
	 * Splits Field's shortcut type-format, such as long_timestamp-millis, into type (long)
	 * and logicalType (timestamp-millis) parts. If the type-format is not shortcut does nothing.
	 */
	private void normalizeFieldType(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		if (field.getType().split("_").length > 1) {
			String[] typeAndLogicalType = field.getType().split("_");
			field.setLogicalType(typeAndLogicalType[1]);
			field.setType(typeAndLogicalType[0]);
		}
	}

	/**
	 * Helper class that allow to check if a column name is already present in the target table.
	 * The column can either exist as part of the initial avro/datatype schema or added as time/metadata attribute.
	 */
	private static class ColumnExistenceChecker {

		private final Map<String, String> newAddedFields;

		ColumnExistenceChecker(DataType dataType) {
			this.newAddedFields = new ConcurrentHashMap<>();
			((RowType) dataType.getLogicalType()).getFields()
					.forEach(rt -> newAddedFields.put(rt.getName(), ""));
		}

		public void markColumnNameAsExisting(String fieldName) {
			this.newAddedFields.put(fieldName, "");
		}

		public boolean isColumnExist(String fieldName) {
			return this.newAddedFields.containsKey(fieldName);
		}
	}
}
