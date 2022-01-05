package com.vmware.tanzu.streaming.runtime;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

@Component
public class ConfigMapUpdater {
	private static final Logger LOG = LoggerFactory.getLogger(ConfigMapUpdater.class);

	private final CoreV1Api coreV1Api;
	private final Lister<V1ConfigMap> configMapLister;
	private final String clusterStreamConfigMapKey;
	private final String clusterStreamNamespace;
	private final ObjectMapper yamlMapper;

	public ConfigMapUpdater(@Value("${streaming-runtime.configMapKey}") String clusterStreamConfigMapKey,
			@Value("${streaming-runtime.namespace}") String clusterStreamNamespace,
			CoreV1Api coreV1Api, Lister<V1ConfigMap> configMapLister,
			ObjectMapper yamlMapper) {
		this.coreV1Api = coreV1Api;
		this.configMapLister = configMapLister;
		this.clusterStreamConfigMapKey = clusterStreamConfigMapKey;
		this.clusterStreamNamespace = clusterStreamNamespace;
		this.yamlMapper = yamlMapper;
	}

	public V1ConfigMap createConfigMap(V1OwnerReference clusterStream) throws ApiException, JsonProcessingException {
		LOG.debug("Creating config map {}/{}", clusterStreamNamespace, clusterStream.getName());
		StreamsProperties properties = new StreamsProperties(Collections.emptyList());
		V1ConfigMap configMap = new V1ConfigMap()
				.apiVersion("v1")
				.kind("ConfigMap")
				.metadata(new V1ObjectMeta()
						.name(clusterStream.getName())
						.namespace(clusterStreamNamespace)
						.ownerReferences(singletonList(clusterStream)));
		addDataToConfigMap(configMap, properties);
		return coreV1Api.createNamespacedConfigMap(clusterStreamNamespace, configMap, null, null, null);
	}

	public V1ConfigMap removeStream(String streamNameToRemove, String clusterStreamName) throws ApiException, JsonProcessingException {
		LOG.debug("Removing Stream {} from config map {}", streamNameToRemove, clusterStreamName);
		StreamsProperties properties = getExistingStreams(clusterStreamName);
		properties.getStreams().removeIf(stream -> stream.getName().equalsIgnoreCase(streamNameToRemove));
		return updateConfigMap(clusterStreamName, properties);
	}

	public StreamsProperties getExistingStreams(String clusterStreamName) throws JsonProcessingException {
		V1ConfigMap configMap = getExistingConfigMap(clusterStreamName);
		String serializedStreams = configMap.getData().get(this.clusterStreamConfigMapKey);
		return yamlMapper.readValue(serializedStreams, ApplicationYaml.class).getStreamsProperties();
	}

	public boolean isStreamExist(String streamName, String clusterStreamName) {
		try {
			return getExistingStreams(clusterStreamName).getStreams().stream()
					.anyMatch(stream -> stream.getName().equalsIgnoreCase(streamName));
		}
		catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return false;
	}

	public V1ConfigMap updateConfigMap(String clusterStreamName, StreamsProperties properties) throws JsonProcessingException, ApiException {
		V1ConfigMap configMap = addDataToConfigMap(getExistingConfigMap(clusterStreamName), properties);
		return coreV1Api.replaceNamespacedConfigMap(configMap.getMetadata().getName(), clusterStreamNamespace,
				configMap, null, null, null);
	}

	private V1ConfigMap addDataToConfigMap(V1ConfigMap configMap, StreamsProperties properties) throws JsonProcessingException {
		String serializedContent = yamlMapper.writeValueAsString(new ApplicationYaml(properties));

		return configMap.data(singletonMap(clusterStreamConfigMapKey, serializedContent));
	}

	public boolean configMapExists(String clusterStreamName) {
		LOG.debug("Checking if config map {}/{} exists", clusterStreamNamespace, clusterStreamName);
		return (getExistingConfigMap(clusterStreamName) != null);
	}

	private V1ConfigMap getExistingConfigMap(String clusterStreamName) {
		return configMapLister.namespace(clusterStreamNamespace).get(clusterStreamName);
	}

	public static class ApplicationYaml {

		private StreamsProperties streamsProperties;

		public ApplicationYaml() {
		}

		public ApplicationYaml(StreamsProperties streamsProperties) {
			this.streamsProperties = streamsProperties;
		}

		public void setStreamsProperties(StreamsProperties streamsProperties) {
			this.streamsProperties = streamsProperties;
		}

		public StreamsProperties getStreamsProperties() {
			return streamsProperties;
		}
	}

	public static class StreamsProperties {

		private List<Stream> streams;

		public StreamsProperties() {
		}

		public StreamsProperties(List<Stream> streams) {
			this.streams = streams;
		}

		public void setStreams(List<Stream> streams) {
			this.streams = streams;
		}

		public List<Stream> getStreams() {
			return streams;
		}
	}

	public static class Stream {

		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
