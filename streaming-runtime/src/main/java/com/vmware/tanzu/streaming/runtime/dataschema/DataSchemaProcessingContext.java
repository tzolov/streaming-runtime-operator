package com.vmware.tanzu.streaming.runtime.dataschema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamStatusStorageAddressServers;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchema;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaMetadataFields;
import com.vmware.tanzu.streaming.models.V1alpha1StreamSpecDataSchemaTimeAttributes;
import io.kubernetes.client.openapi.ApiException;

import org.springframework.util.StringUtils;

public class DataSchemaProcessingContext {

	public static final String META_SCHEMA_TYPE = "meta-schema";
	public static final String STREAM_STATUS_SERVER_PREFIX = "stream.status.server.";

	private final String streamName;
	private final String streamProtocol;
	private final V1alpha1StreamSpecDataSchema streamDataSchema;
	private final java.util.Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> metadataFields;
	private final Map<String, String> timeAttributes;
	private final Map<String, String> options;

	private DataSchemaProcessingContext(String streamName, String streamProtocol, V1alpha1StreamSpecDataSchema streamDataSchema,
			Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> metadataFields,
			Map<String, String> timeAttributes,
			Map<String, String> options) {
		this.streamName = streamName;
		this.streamProtocol = streamProtocol;
		this.streamDataSchema = streamDataSchema;
		this.metadataFields = metadataFields;
		this.timeAttributes = timeAttributes;
		this.options = options;
	}

	public static DataSchemaProcessingContext of(V1alpha1Stream stream) throws ApiException {
		V1alpha1StreamSpecDataSchema streamDataSchema = stream.getSpec().getDataSchema();
		if (streamDataSchema == null) {
			throw new ApiException("Missing dataSchema for: " + stream.getMetadata().getName());
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

		// Options defined in the Stream's CR definition.
		Map<String, String> dataSchemaContextOptions =
				Optional.ofNullable(streamDataSchema.getOptions()).orElse(new HashMap<>());

		// Add all Stream status server variables as options with stream.status.server. prefix.
		// Covert the storage address server information into Flink SQL connector WITH section.
		V1alpha1ClusterStreamStatusStorageAddressServers server = stream.getStatus()
				.getStorageAddress().getServers().values().iterator().next();
		Map<String, String> streamStatusServerVariables = server.getVariables();
		streamStatusServerVariables.entrySet().stream().forEach(v -> {
			dataSchemaContextOptions.put(STREAM_STATUS_SERVER_PREFIX + v.getKey(), v.getValue());
		});

		return new DataSchemaProcessingContext(
				stream.getMetadata().getName(),
				stream.getSpec().getProtocol(),
				streamDataSchema,
				metadataFields,
				timeAttributes,
				dataSchemaContextOptions);
	}

	public V1alpha1StreamSpecDataSchema getStreamDataSchema() {
		return streamDataSchema;
	}

	public Map<String, V1alpha1StreamSpecDataSchemaMetadataFields> getMetadataFields() {
		return metadataFields;
	}

	public Map<String, String> getTimeAttributes() {
		return timeAttributes;
	}

	public Map<String, String> getOptions() {
		return options;
	}

	public String getStreamName() {
		return streamName;
	}

	public String getStreamProtocol() {
		return streamProtocol;
	}

	public boolean isDataSchemaAvailable() {
		return StringUtils.hasText(this.getDataSchemaType());
	}

	public String getDataSchemaType() {
		if (this.streamDataSchema.getInline() != null) {
			return this.streamDataSchema.getInline().getType();
		}
		else if (streamDataSchema.getSchema() != null) {
			return META_SCHEMA_TYPE;
		}
		return null;
	}
}
