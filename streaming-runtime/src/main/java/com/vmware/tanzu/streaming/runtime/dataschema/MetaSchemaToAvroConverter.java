package com.vmware.tanzu.streaming.runtime.dataschema;

import java.util.Optional;

import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaMetadataFields;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaSchema;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Converts the Meta-Schema defined in the Stream CRD into Avro Schema.
 * 	The meta-schema time attributes and metadata fields are stripped from the Avro schema and added to the
 * 	metadataFields and timeAttributes instead.
 */
@Component
public class MetaSchemaToAvroConverter implements DataSchemaAvroConverter {

	public static final String PROC_TIME = "proctime";

	public static final String TYPE = DataSchemaProcessingContext.META_SCHEMA_TYPE;

	@Override
	public String getSupportedDataSchemaType() {
		return TYPE;
	}

	/**
	 * Mind that the context is mutable
	 * @param context
	 * @return
	 */
	@Override
	public Schema toAvro(DataSchemaProcessingContext context) {

		Assert.notNull(context.getStreamDataSchema().getSchema(), String.format("Missing schema Meta-Schema"));

		V1alpha1StreamSpecDataSchemaSchema streamSchema = context.getStreamDataSchema().getSchema();

		Assert.notNull(streamSchema, "Missing Meta-Schema");

		// Start Avro Schema Builder.
		SchemaBuilder.FieldAssembler<Schema> recordFieldBuilder = SchemaBuilder.record(streamSchema.getName())
				.namespace(streamSchema.getNamespace()).fields();

		for (V1alpha1StreamSpecDataSchemaMetadataFields field : streamSchema.getFields()) {

			// Normalize the field's type and logicalType fields
			normalizeFieldType(field);

			// If a time-attribute field that is not defined already in the data-schema's outer time-attributes
			// section add it map of time attributes.
			if (isTimeAttribute(field) && !context.getTimeAttributes().containsKey(field.getName())) {
				// the field should not override the outer time attribute!
				context.getTimeAttributes().put(field.getName(), field.getWatermark());
			}

			// if a metadata field that is not defined in the CD's outer metadata section add the field to the list
			// of metadata fields.
			// The meta-schema field should not override the outer metadata attribute!
			if (field.getMetadata() != null && !context.getMetadataFields().containsKey(field.getName())) {
				context.getMetadataFields().put(field.getName(), field);
			}

			// Unless a metadata or proctime field add it to the target Avro schema.
			// Note that by design the resolved data-schema should contain only data related fields. The metadata mapping
			// or time-attribute related information is managed by final schema resolved for particular
			// target aggregator such as Flink, KSQL, ...
			if (!context.getMetadataFields().containsKey(field.getName()) && !isProcTimeAttribute(field)) {
				recordFieldBuilder
						.name(field.getName())
						.type(fieldToAvroType(field))
						.noDefault();
			}
		}

		return recordFieldBuilder.endRecord();
	}

	/**
	 * Splits Field's shortcut type-format, such as long_timestamp-millis, into type (long)
	 * and logicalType (timestamp-millis) parts. If the type-format is not shortcut does nothing.
	 */
	public static void normalizeFieldType(V1alpha1StreamSpecDataSchemaMetadataFields field) {
		if (field.getType().split("_").length > 1) {
			String[] typeAndLogicalType = field.getType().split("_");
			field.setLogicalType(typeAndLogicalType[1]);
			field.setType(typeAndLogicalType[0]);
		}
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
		return PROC_TIME.equalsIgnoreCase(field.getType());
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
}
