package com.vmware.tanzu.streaming.runtime;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vmware.tanzu.streaming.apis.StreamingTanzuVmwareComV1alpha1Api;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamStatusStorageAddressServers;
import com.vmware.tanzu.streaming.models.V1alpha1Processor;
import com.vmware.tanzu.streaming.models.V1alpha1ProcessorSpecInputsSources;
import com.vmware.tanzu.streaming.models.V1alpha1ProcessorSpecTemplateSpecContainers;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.models.V1alpha1StreamList;
import com.vmware.tanzu.streaming.runtime.config.ProcessorConfiguration;
import com.vmware.tanzu.streaming.runtime.uitil.DataSchemaToDdlConverter;
import com.vmware.tanzu.streaming.runtime.uitil.QueryPlaceholderResolver;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.extended.event.EventType;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerBuilder;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1EnvVar;
import io.kubernetes.client.openapi.models.V1EnvVarBuilder;
import io.kubernetes.client.openapi.models.V1KeyToPathBuilder;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeBuilder;
import io.kubernetes.client.util.PatchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
public class ProcessorReconciler implements Reconciler {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessorReconciler.class);

	private static final Resource PROCESSOR_DEPLOYMENT_TEMAPLATE =
			toResource("classpath:manifests/processor/streaming-runtime-processor-deployment.yaml");

	private static final Resource SQL_AGGREGATION_CONTAINER_TEMPLATE =
			toResource("classpath:manifests/processor/sql-aggregation-container-template.yaml");

	private static final boolean REQUEUE = true;

	private final Lister<V1alpha1Processor> processorLister;
	private final CoreV1Api coreV1Api;
	private final EventRecorder eventRecorder;
	private final AppsV1Api appsV1Api;
	private final ObjectMapper yamlMapper;
	private final ConfigMapUpdater configMapUpdater;
	private final StreamingTanzuVmwareComV1alpha1Api api;
	private final DataSchemaToDdlConverter dataSchemaToDdlConverter;

	public ProcessorReconciler(SharedIndexInformer<V1alpha1Processor> processorInformer,
			StreamingTanzuVmwareComV1alpha1Api api,
			CoreV1Api coreV1Api,
			EventRecorder eventRecorder,
			AppsV1Api appsV1Api,
			ObjectMapper yamlMapper,
			ConfigMapUpdater configMapUpdater,
			DataSchemaToDdlConverter dataSchemaToDdlConverter) {

		this.api = api;
		this.processorLister = new Lister<>(processorInformer.getIndexer());
		this.coreV1Api = coreV1Api;
		this.eventRecorder = eventRecorder;
		this.appsV1Api = appsV1Api;
		this.yamlMapper = yamlMapper;
		this.configMapUpdater = configMapUpdater;
		this.dataSchemaToDdlConverter = dataSchemaToDdlConverter;
	}

	@Override
	public Result reconcile(Request request) {

		String processorName = request.getName();
		String processorNamespace = request.getNamespace();

		V1alpha1Processor processor = this.processorLister.namespace(processorNamespace).get(processorName);

		if (processor == null) {
			LOG.error(String.format("Missing Processor: %s/%s", processorNamespace, processorName));
			return new Result(!REQUEUE);
		}

		try {
			final boolean toDelete = processor.getMetadata().getDeletionTimestamp() != null;

			if (!toDelete) {
				List<V1alpha1Stream> inputStreams = getStreams(processor, processor.getSpec().getInputs().getSources());
				List<V1alpha1Stream> outputStreams = getStreams(processor, processor.getSpec().getOutputs());


				List<String> sqlQueries = processor.getSpec().getInputs().getQuery();

				// Extract the stream names used in the SQL statements and map those to the in-query placeholders.
				// Later the placeholders will be replaced by the Schema (e.g. Tables) names.
				Map<String, String> sqlPlaceholderToStreamName =
						isQueryPresent(processor) ? QueryPlaceholderResolver.extractPlaceholders(sqlQueries) : new HashMap<>();

				if (!isProcessorPodExists(processor)) {
					createProcessorDeploymentIfMissing(processor, inputStreams, outputStreams);
				}

				if (isQueryPresent(processor) && !CollectionUtils.isEmpty(sqlPlaceholderToStreamName)) {
					List<String> ddlStatements = new ArrayList<>();
					Map<String, String> placeholderToSchemaNameMap = new HashMap<>();
					for (Map.Entry<String, String> e : sqlPlaceholderToStreamName.entrySet()) {
						String queryPlaceholder = e.getKey();
						String streamName = e.getValue();
						V1alpha1Stream stream = getStream(processor, streamName);
						DataSchemaToDdlConverter.TableDdlInfo tableDdlInfo = dataSchemaToDdlConverter.createTableDdl(stream);
						ddlStatements.add(tableDdlInfo.getTableDdl());
						placeholderToSchemaNameMap.put(queryPlaceholder, tableDdlInfo.getTableName());
					}

					List<String> resolvedQueries = QueryPlaceholderResolver.resolveQueries(sqlQueries, placeholderToSchemaNameMap);

					ddlStatements.addAll(resolvedQueries);

					createSqlAppConfigMap(processor, ddlStatements);
				}

				// Status update
				if (isProcessorPodRunning(processor)) {
					setProcessorStatus(processor, "true", "ProcessorDeployed");
				}
				else {
					setProcessorStatus(processor, "false", "ProcessorDeploying");
					return new Result(REQUEUE, Duration.of(15, ChronoUnit.SECONDS));
				}
			}
		}
		catch (Exception e) {
			logFailureEvent(processor, processorNamespace, e.getMessage(), e);
			return new Result(REQUEUE, Duration.of(15, ChronoUnit.SECONDS));
		}

		return new Result(!REQUEUE);
	}


	private boolean isQueryPresent(V1alpha1Processor processor) {
		return !CollectionUtils.isEmpty(processor.getSpec().getInputs().getQuery());
	}

	private List<V1alpha1Stream> getStreams(V1alpha1Processor processor,
			List<V1alpha1ProcessorSpecInputsSources> streamDefs) throws ApiException {

		List<V1alpha1Stream> streams = new ArrayList<>();
		for (V1alpha1ProcessorSpecInputsSources sd : streamDefs) {
			V1alpha1Stream stream = getStream(processor, sd.getName());
			streams.add(stream);
		}
		return streams;
	}

	private V1alpha1Stream getStream(V1alpha1Processor processor, String streamName) throws ApiException {

		V1alpha1StreamList streamList = api.listStreamForAllNamespaces(null, null,
				"metadata.name=" + streamName, null, null,
				null, null, null, null, null);

		if (CollectionUtils.isEmpty(streamList.getItems())) {
			setProcessorStatus(processor, "false", "ProcessorMissingStream");
			throw new ApiException("Missing Stream: " + streamName);
		}

		// Should be only one. Fallback to the first if more than one.
		// TODO perhaps we should add namespace as well?
		if (streamList.getItems().size() > 1) {
			LOG.warn(String.format("Many (%s) Streams with name: %s found! Only the first is used!",
					streamList.getItems().size(), streamName));
		}

		V1alpha1Stream stream = streamList.getItems().get(0);

		if (stream == null) {
			setProcessorStatus(processor, "false", "ProcessorMissingStream");
			throw new ApiException("MissingStream: " + streamName);
		}
		if (!isStreamReady(stream)) {
			setProcessorStatus(processor, "false", "ProcessorStreamNotReady");
			throw new ApiException("StreamNotReady: " + streamName);
		}

		return stream;
	}

	public boolean isProcessorPodExists(V1alpha1Processor processor) {
		try {
			return coreV1Api.listNamespacedPod(processor.getMetadata().getNamespace(), null, null, null,
					null,
					"app in (streaming-runtime-processor),streaming-runtime=" + processor.getMetadata().getName(),
					null, null, null, null, null).getItems().size() == 1;
		}
		catch (ApiException e) {
			LOG.warn("Failed to check the processor Pod existence", e);
		}
		return false;
	}

	private boolean isProcessorPodRunning(V1alpha1Processor processor) {
		try {
			return coreV1Api.listNamespacedPod(processor.getMetadata().getNamespace(), null, null, null,
					"status.phase=Running",
					"app in (streaming-runtime-processor),streaming-runtime=" + processor.getMetadata().getName(),
					null, null, null, null, null).getItems().size() == 1;
		}
		catch (ApiException e) {
			LOG.warn("Failed to check if the processor Pod running", e);
		}
		return false;
	}

	private void createProcessorDeploymentIfMissing(V1alpha1Processor processor,
			List<V1alpha1Stream> inputStreams, List<V1alpha1Stream> outputStreams) throws IOException, ApiException {


		V1OwnerReference ownerReference = toOwnerReference(processor);

		LOG.debug("Creating deployment {}/{}", processor.getMetadata().getNamespace(), ownerReference.getName());
		V1Deployment body = yamlMapper.readValue(PROCESSOR_DEPLOYMENT_TEMAPLATE.getInputStream(), V1Deployment.class);
		body.getMetadata().setName("streaming-runtime-processor-" + ownerReference.getName());
		body.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
		body.getSpec().getTemplate().getMetadata().getLabels().put("streaming-runtime", ownerReference.getName());

		// Env variables

		// SPRING_CLOUD_STREAM_BINDINGS_INPUT_DESTINATION - Stream's metadata.name or dedicated key/attribute ?
		// SPRING_CLOUD_STREAM_BINDINGS_INPUT_BINDER - kafka
		// SPRING_CLOUD_STREAM_BINDINGS_OUTPUT_DESTINATION - Stream's metadata.name or dedicated key/attribute ?
		// SPRING_CLOUD_STREAM_BINDINGS_OUTPUT_BINDER - rabbit
		//
		// SPRING_CLOUD_STREAM_KAFKA_BINDER_BROKERS - kafka:9092
		// SPRING_CLOUD_STREAM_KAFKA_BINDER_ZKNODES - kafka-zk:2181
		//
		// SPRING_RABBITMQ_HOST - rabbitmq
		// SPRING_RABBITMQ_PORT - 5672
		// SPRING_RABBITMQ_USERNAME - guest
		// SPRING_RABBITMQ_PASSWORD - guest

		Map<String, String> envs = new HashMap<>();

		// Assumes one input stream
		V1alpha1Stream inputStream = inputStreams.get(0);
		V1alpha1ClusterStreamStatusStorageAddressServers inServer = inputStream.getStatus()
				.getStorageAddress().getServers().values().iterator().next();

		if (inServer.getProtocol().equalsIgnoreCase("kafka")) {
			envs.put("SPRING_CLOUD_STREAM_BINDINGS_INPUT_BINDER", "kafka");
			envs.put("SPRING_CLOUD_STREAM_KAFKA_BINDER_BROKERS", inServer.getVariables().get("brokers"));
			envs.put("SPRING_CLOUD_STREAM_KAFKA_BINDER_ZKNODES", inServer.getVariables().get("zkNodes"));
		}
		else if (inServer.getProtocol().equalsIgnoreCase("rabbitmq")) {
			envs.put("SPRING_CLOUD_STREAM_BINDINGS_INPUT_BINDER", "rabbit");
			envs.put("SPRING_RABBITMQ_HOST", inServer.getVariables().get("host"));
			envs.put("SPRING_RABBITMQ_PORT", inServer.getVariables().get("port"));
			envs.put("SPRING_RABBITMQ_USERNAME", inServer.getVariables().get("username"));
			envs.put("SPRING_RABBITMQ_PASSWORD", inServer.getVariables().get("password"));
		}
		envs.put("SPRING_CLOUD_STREAM_BINDINGS_INPUT_DESTINATION", inputStream.getMetadata().getName()); // TODO
		envs.put("SPRING_CLOUD_STREAM_FUNCTION_BINDINGS_PROXY-IN-0", "input"); // TODO


		// Assumes one output stream
		V1alpha1Stream outputStream = outputStreams.get(0);
		V1alpha1ClusterStreamStatusStorageAddressServers outServer = outputStream.getStatus()
				.getStorageAddress().getServers().values().iterator().next();

		if (outServer.getProtocol().equalsIgnoreCase("kafka")) {
			envs.put("SPRING_CLOUD_STREAM_BINDINGS_OUTPUT_BINDER", "kafka");
			envs.put("SPRING_CLOUD_STREAM_KAFKA_BINDER_BROKERS", outServer.getVariables().get("brokers"));
			envs.put("SPRING_CLOUD_STREAM_KAFKA_BINDER_ZKNODES", outServer.getVariables().get("zkNodes"));
		}
		else if (outServer.getProtocol().equalsIgnoreCase("rabbitmq")) {
			envs.put("SPRING_CLOUD_STREAM_BINDINGS_OUTPUT_BINDER", "rabbit");
			envs.put("SPRING_RABBITMQ_HOST", outServer.getVariables().get("host"));
			envs.put("SPRING_RABBITMQ_PORT", outServer.getVariables().get("port"));
			envs.put("SPRING_RABBITMQ_USERNAME", outServer.getVariables().get("username"));
			envs.put("SPRING_RABBITMQ_PASSWORD", outServer.getVariables().get("password"));
		}
		envs.put("SPRING_CLOUD_STREAM_BINDINGS_OUTPUT_DESTINATION", outputStream.getMetadata().getName()); // TODO
		envs.put("SPRING_CLOUD_STREAM_FUNCTION_BINDINGS_PROXY-OUT-0", "output"); // TODO

		V1Container container = body.getSpec().getTemplate().getSpec().getContainers().stream()
				.filter(c -> "multibinder-grpc".equalsIgnoreCase(c.getName()))
				.findFirst().get();

		List<V1EnvVar> containerVariables = (container.getEnv() != null) ? container.getEnv() : new ArrayList<>();

		containerVariables.addAll(envs.entrySet().stream()
				.map(e -> new V1EnvVarBuilder()
						.withName(e.getKey())
						.withValue(e.getValue())
						.build())
				.collect(Collectors.toList()));

		// Add additional (UDF) containers
		for (V1alpha1ProcessorSpecTemplateSpecContainers procContainer :
				processor.getSpec().getTemplate().getSpec().getContainers()) {

			body.getSpec().getTemplate().getSpec().getContainers().add(
					new V1ContainerBuilder()
							.withName(procContainer.getName())
							.withImage(procContainer.getImage())
							.withEnv(procContainer.getEnv().stream()
									.map(e -> new V1EnvVarBuilder()
											.withName(e.getName())
											.withValue(e.getValue())
											.build())
									.collect(Collectors.toList()))
							.build());
		}

		// In case of SQL input enable the sql-aggregator (e.g. Flink) container
		if (!CollectionUtils.isEmpty(processor.getSpec().getInputs().getQuery())) {
			List<V1Volume> volumes = body.getSpec().getTemplate().getSpec()
					.getVolumes();
			if (CollectionUtils.isEmpty(volumes)) {
				volumes = new ArrayList<>();
			}
			volumes.add(new V1VolumeBuilder()
					.withName("config")
					.withNewConfigMap()
					.withName(processor.getMetadata().getName())
					.withItems(List.of(new V1KeyToPathBuilder()
							.withKey("application.yaml")
							.withPath("application.yaml")
							.build()))
					.endConfigMap()
					.build());
			body.getSpec().getTemplate().getSpec().setVolumes(volumes);

			V1Container sqlAggregatorContainer = yamlMapper.readValue(SQL_AGGREGATION_CONTAINER_TEMPLATE.getInputStream(), V1Container.class);
			sqlAggregatorContainer.setEnv(List.of(new V1EnvVarBuilder()
							.withName("SQL_AGGREGATION_KAFKASERVER")
							.withValue("kafka." + processor.getMetadata().getNamespace() + ".svc.cluster.local:9092") // TODO
							//.withValue("localhost:9094") // TODO
							.build(),
					new V1EnvVarBuilder()
							.withName("SQL_AGGREGATION_SCHEMAREGISTRY")
							.withValue("http://s-registry." + processor.getMetadata()
									.getNamespace() + ".svc.cluster.local:8081") // TODO
							//.withValue("http://localhost:8081") // TODO
							.build()));
			body.getSpec().getTemplate().getSpec().getContainers().add(sqlAggregatorContainer);
		}

		appsV1Api.createNamespacedDeployment(processor.getMetadata().getNamespace(), body, null, null, null);
	}

	private boolean isStreamReady(V1alpha1Stream stream) {
		if (stream.getStatus() == null || stream.getStatus().getConditions() == null) {
			return false;
		}

		return stream.getStatus().getConditions().stream()
				.filter(c -> "Ready".equalsIgnoreCase(c.getType()))
				.allMatch(c -> "true".equalsIgnoreCase(c.getStatus()));
	}

	private V1OwnerReference toOwnerReference(V1alpha1Processor processor) {
		return new V1OwnerReference().controller(true)
				.name(processor.getMetadata().getName())
				.uid(processor.getMetadata().getUid())
				.kind(processor.getKind())
				.apiVersion(processor.getApiVersion())
				.blockOwnerDeletion(true);
	}

	private void logFailureEvent(V1alpha1Processor processor, String namespace, String reason, Exception e) {
		String message = String.format("Failed to deploy Processor %s: %s", processor.getMetadata().getName(), reason);
		LOG.warn(message, e);
		eventRecorder.logEvent(
				EventRecorder.toObjectReference(processor).namespace(namespace),
				null,
				ProcessorConfiguration.PROCESSOR_CONTROLLER_NAME,
				e.getClass().getName(),
				message + ": " + e.getMessage(),
				EventType.Warning);
	}

	public void setProcessorStatus(V1alpha1Processor processor, String status, String reason) {

		if (!hasProcessorConditionChanged(processor, status, reason)) {
			return;
		}

		String patch = String.format("" +
						"{\"status\": " +
						"  {\"conditions\": " +
						"      [{ \"type\": \"%s\", \"status\": \"%s\", \"lastTransitionTime\": \"%s\", \"reason\": \"%s\"}]" +
						"  }" +
						"}",
				"Ready", status, ZonedDateTime.now(ZoneOffset.UTC), reason);
		try {
			PatchUtils.patch(
					V1alpha1Processor.class,
					() -> api.patchNamespacedProcessorStatusCall(
							processor.getMetadata().getName(),
							processor.getMetadata().getNamespace(),
							new V1Patch(patch), null, null, null, null),
					V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
					api.getApiClient());
		}
		catch (ApiException e) {
			LOG.error("Status API call failed: {}: {}, {}, with patch {}", e.getCode(), e.getMessage(), e.getResponseBody(), patch);
		}
	}

	private boolean hasProcessorConditionChanged(V1alpha1Processor processor, String newReadyStatus, String newStatusReason) {
		if (processor.getStatus() == null || processor.getStatus().getConditions() == null) {
			return true;
		}

		return !processor.getStatus().getConditions().stream()
				.filter(condition -> "Ready".equalsIgnoreCase(condition.getType()))
				.allMatch(condition -> newReadyStatus.equalsIgnoreCase(condition.getStatus())
						&& newStatusReason.equalsIgnoreCase(condition.getReason()));
	}

	private static Resource toResource(String uri) {
		return new DefaultResourceLoader().getResource(uri);
	}

	private V1ConfigMap createSqlAppConfigMap(V1alpha1Processor processor,
			List<String> sqlQueriesAndDdl) throws ApiException {

		String debugQuery = "";
		List<Integer> explainIds = new ArrayList<>();
		if (processor.getSpec().getInputs().getDebug() != null) {
			debugQuery = processor.getSpec().getInputs().getDebug().getQuery();
			explainIds = processor.getSpec().getInputs().getDebug().getExplain();
		}
		Aggregation sqlAggregation = new Aggregation(sqlQueriesAndDdl, debugQuery, explainIds);
		ApplicationYaml appYaml = new ApplicationYaml(new Sql(sqlAggregation));

		String configMapName = processor.getMetadata().getName();
		String configMapNamespace = processor.getMetadata().getNamespace();
		String configMapKey = "application.yaml";
		try {
			String serializedContent = yamlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(appYaml);
			if (configMapUpdater.configMapExists(configMapName, configMapNamespace)) {
				return configMapUpdater.updateConfigMap(configMapName, configMapNamespace, configMapKey, serializedContent);
			}
			else {
				return configMapUpdater.createConfigMap(toOwnerReference(processor),
						configMapName, configMapNamespace, configMapKey, serializedContent);
			}
		}
		catch (JsonProcessingException e) {
			LOG.error("Failed to serialize processor config map", e);
			throw new ApiException(e);
		}
	}

	public static class ApplicationYaml {

		private Sql sql;

		public ApplicationYaml() {
		}

		public ApplicationYaml(Sql sql) {
			this.sql = sql;
		}

		public Sql getSql() {
			return sql;
		}

		public void setSql(Sql sql) {
			this.sql = sql;
		}
	}

	public static class Sql {

		private Aggregation aggregation;

		Sql() {
		}

		public Sql(Aggregation aggregation) {
			this.aggregation = aggregation;
		}

		public Aggregation getAggregation() {
			return aggregation;
		}

		public void setAggregation(Aggregation aggregation) {
			this.aggregation = aggregation;
		}
	}

	public static class Aggregation {

		private List<String> executeSql;
		private String continuousQuery;
		private List<Integer> explainStatements;

		public Aggregation() {
		}

		public Aggregation(List<String> executeSql, String continuousQuery, List<Integer> explainStatements) {
			this.executeSql = executeSql;
			this.continuousQuery = continuousQuery;
			this.explainStatements = explainStatements;
		}

		public List<String> getExecuteSql() {
			return executeSql;
		}

		public void setExecuteSql(List<String> executeSql) {
			this.executeSql = executeSql;
		}

		public String getContinuousQuery() {
			return continuousQuery;
		}

		public void setContinuousQuery(String continuousQuery) {
			this.continuousQuery = continuousQuery;
		}

		public List<Integer> getExplainStatements() {
			return explainStatements;
		}

		public void setExplainStatements(List<Integer> explainStatements) {
			this.explainStatements = explainStatements;
		}
	}
}
