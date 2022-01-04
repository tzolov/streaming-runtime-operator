package com.vmware.tanzu.streaming.runtime;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmware.tanzu.streaming.apis.StreamingTanzuVmwareComV1alpha1Api;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStream;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamList;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamSpecStorageServers;
import com.vmware.tanzu.streaming.models.V1alpha1Processor;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.models.V1alpha1StreamList;
import com.vmware.tanzu.streaming.runtime.config.ClusterStreamConfiguration;
import com.vmware.tanzu.streaming.runtime.protocol.ProtocolDeploymentEditor;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.extended.event.EventType;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.PatchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class ProcessorReconciler implements Reconciler {

	private static final Logger LOG = LoggerFactory.getLogger(ProcessorReconciler.class);
	private static final boolean REQUEUE = true;

	private final Lister<V1alpha1Processor> processorLister;
	private final EventRecorder eventRecorder;
	private final ConfigMapUpdater configMapUpdater;
	private final String streamingRuntimeNameSpace;
	private final StreamingTanzuVmwareComV1alpha1Api api;

	public ProcessorReconciler(SharedIndexInformer<V1alpha1Processor> processorInformer,
			@Value("${streaming-runtime.namespace}") String streamingRuntimeNameSpace,
			StreamingTanzuVmwareComV1alpha1Api api, EventRecorder eventRecorder, ConfigMapUpdater configMapUpdater) {
		this.streamingRuntimeNameSpace = streamingRuntimeNameSpace;

		this.api = api;
		this.processorLister = new Lister<>(processorInformer.getIndexer());
		this.eventRecorder = eventRecorder;
		this.configMapUpdater = configMapUpdater;
	}

	@Override
	public Result reconcile(Request request) {

		V1alpha1Processor processor = this.processorLister.get(request.getName());
		String namespace = (StringUtils.hasText(request.getNamespace())) ? request.getNamespace() : "default";

		final boolean toAdd = (processor.getMetadata() != null) && (processor.getMetadata().getGeneration() == null
				|| processor.getMetadata().getGeneration() == 1) && (processor.getStatus() == null);

		final boolean toUpdate = (processor.getMetadata() != null) && (processor.getMetadata().getGeneration() != null
				&& processor.getMetadata().getGeneration() > 1
				&& processor.getMetadata().getDeletionTimestamp() == null);

		try {
			V1OwnerReference ownerReference = toOwnerReference(processor);
			Set<String> inputNames = processor.getSpec().getInputs().getSources().stream().map(s ->
					s.getName()).collect(Collectors.toSet());
			Set<String> outputNames = processor.getSpec().getOutputs().stream().map(s ->
					s.getName()).collect(Collectors.toSet());
			//for (V1alpha1ClusterStreamSpecStorageServers server : processor.getSpec().getStorage().getServers()) {
			//	ProtocolDeploymentEditor protocolDeploymentEditor = this.protocolDeploymentEditors.get(server.getProtocol());
			//	if (toAdd) {
			//		if (!configMapUpdater.configMapExists(ownerReference.getName())) {
			//			configMapUpdater.createConfigMap(ownerReference);
			//		}
			//		if (protocolDeploymentEditor.createMissingServicesAndDeployments(ownerReference, namespace)) {
			//			setClusterStreamStatus(processor, "Ready", "false",
			//					"create-" + protocolDeploymentEditor.getProtocolName() + "-cluster",
			//					protocolDeploymentEditor.storageAddress(ownerReference, namespace));
			//		}
			//		else {
			//			if (protocolDeploymentEditor.isAllRunning(ownerReference, namespace)) {
			//				if (!processor.getStatus().getConditions().stream().map(c -> c.getStatus()).allMatch(s -> s.equalsIgnoreCase("true"))) {
			//					setClusterStreamStatus(processor, "Ready", "true",
			//							"running-" + protocolDeploymentEditor.getProtocolName() + "-cluster",
			//							protocolDeploymentEditor.storageAddress(ownerReference, namespace));
			//				}
			//			}
			//			else {
			//				return new Result(REQUEUE, Duration.of(30, ChronoUnit.SECONDS));
			//			}
			//		}
			//	}
			//	else if (toUpdate) {
			//		//TODO
			//		System.out.println("UPDATE");
			//	}
			//}
		//}
		//catch (ApiException e) {
		//	if (e.getCode() == 409) {
		//		LOG.info("Required subresource is already present, skip creation.");
		//		return new Result(!REQUEUE);
		//	}
		//	logFailureEvent(processor, namespace, e.getCode() + " - " + e.getResponseBody(), e);
		//	return new Result(REQUEUE);
		}
		catch (Exception e) {
			logFailureEvent(processor, namespace, e.getMessage(), e);
			return new Result(REQUEUE);
		}
		return new Result(!REQUEUE);
	}

	private V1alpha1Stream findStream(String streamName, String namespace) throws ApiException {
		String fieldSelector = "metadata.name=" + streamName;
		V1alpha1StreamList streamList = api.listNamespacedStream(
				namespace, null, null, null, fieldSelector, null, null,
				null, null, null, false
		);
		// should only be one?
		return streamList.getItems().size() > 0 ? streamList.getItems().get(0) : null;
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
		LOG.error(message, e);
		eventRecorder.logEvent(
				EventRecorder.toObjectReference(processor).namespace(namespace),
				null,
				ClusterStreamConfiguration.CLUSTER_STREAM_CONTROLLER_NAME,
				e.getClass().getName(),
				message + ": " + e.getMessage(),
				EventType.Warning);
	}

	public void setClusterStreamStatus(V1alpha1Processor processor, String type, String status, String reason,
			String storageAddress) {

		String patch = String.format("" +
						"{\"status\": " +
						"  {\"conditions\": " +
						"      [{ \"type\": \"%s\", \"status\": \"%s\", \"lastTransitionTime\": \"%s\", \"reason\": \"%s\"}]," +
						"     %s" +
						"  }" +
						"}",
				type, status, ZonedDateTime.now(ZoneOffset.UTC), reason, storageAddress);
		try {
			PatchUtils.patch(V1alpha1ClusterStream.class,
					() -> api.patchClusterStreamStatusCall(
							processor.getMetadata().getName(),
							new V1Patch(patch), null, null, null, null),
					V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
					api.getApiClient());
		}
		catch (ApiException e) {
			LOG.error("Status API call failed: {}: {}, {}, with patch {}", e.getCode(), e.getMessage(), e.getResponseBody(), patch);
		}
	}
}
