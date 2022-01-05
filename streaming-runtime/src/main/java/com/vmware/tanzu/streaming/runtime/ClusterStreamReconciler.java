package com.vmware.tanzu.streaming.runtime;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.vmware.tanzu.streaming.apis.StreamingTanzuVmwareComV1alpha1Api;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStream;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamSpecStorageServers;
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

import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

@Component
public class ClusterStreamReconciler implements Reconciler {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterStreamReconciler.class);
	private static final boolean REQUEUE = true;

	private final Lister<V1alpha1ClusterStream> clusterStreamLister;
	private final Map<String, ProtocolDeploymentEditor> protocolDeploymentEditors;
	private final EventRecorder eventRecorder;
	private final ConfigMapUpdater configMapUpdater;
	private final StreamingTanzuVmwareComV1alpha1Api api;

	public ClusterStreamReconciler(SharedIndexInformer<V1alpha1ClusterStream> clusterStreamInformer,
			ProtocolDeploymentEditor[] protocolDeploymentEditors, StreamingTanzuVmwareComV1alpha1Api api,
			EventRecorder eventRecorder, ConfigMapUpdater configMapUpdater) {

		this.api = api;
		this.clusterStreamLister = new Lister<>(clusterStreamInformer.getIndexer());
		this.protocolDeploymentEditors =
				Stream.of(protocolDeploymentEditors)
						.collect(Collectors.toMap(ProtocolDeploymentEditor::getProtocolName, Function.identity()));
		this.eventRecorder = eventRecorder;
		this.configMapUpdater = configMapUpdater;
	}

	@Override
	public Result reconcile(Request request) {

		V1alpha1ClusterStream clusterStream = this.clusterStreamLister.get(request.getName());
		String namespace = (StringUtils.hasText(request.getNamespace())) ? request.getNamespace() : "default";

		if (clusterStream == null) {
			LOG.error(String.format("Missing ClusterStream: %s", request.getName()));
			return new Result(!REQUEUE);
		}

		try {
			final boolean toDelete = clusterStream.getMetadata().getDeletionTimestamp() != null;

			V1OwnerReference ownerReference = toOwnerReference(clusterStream);
			if (!toDelete) {
				for (V1alpha1ClusterStreamSpecStorageServers server : clusterStream.getSpec().getStorage().getServers()) {
					ProtocolDeploymentEditor protocolDeploymentEditor = this.protocolDeploymentEditors.get(server.getProtocol());
					if (!configMapUpdater.configMapExists(ownerReference.getName())) {
						configMapUpdater.createConfigMap(ownerReference);
					}
					protocolDeploymentEditor.createMissingServicesAndDeployments(ownerReference, namespace);
				}
			}
			//Status Update
			List<String> serverAddresses = clusterStream.getSpec().getStorage().getServers().stream()
					.map(server -> this.protocolDeploymentEditors.get(server.getProtocol()))
					.filter(protocol -> protocol.isAllRunning(ownerReference, namespace))
					.map(protocol -> protocol.storageAddress(ownerReference, namespace))
					.filter(Objects::nonNull)
					.collect(Collectors.toList());

			boolean isStatusReady = !CollectionUtils.isEmpty(serverAddresses);
			String readyStatus = isStatusReady ? "true" : "false";
			String reason = isStatusReady ? "ProtocolDeployed" : "ProtocolDeployment";
			String storageAddress = String.format("\"storageAddress\": { \"servers\": { %s } }", String.join(",", serverAddresses));

			setClusterStreamStatus(clusterStream, "Ready", readyStatus, reason, storageAddress);

			if (!isStatusReady) {
				return new Result(REQUEUE, Duration.of(30, ChronoUnit.SECONDS));
			}
		}
		catch (
				ApiException e) {
			if (e.getCode() == 409) {
				LOG.info("Required subresource is already present, skip creation.");
				return new Result(!REQUEUE);
			}
			logFailureEvent(clusterStream, namespace, e.getCode() + " - " + e.getResponseBody(), e);
			return new Result(REQUEUE);
		}
		catch (
				Exception e) {
			logFailureEvent(clusterStream, namespace, e.getMessage(), e);
			return new Result(REQUEUE);
		}
		return new Result(!REQUEUE);
	}

	private V1OwnerReference toOwnerReference(V1alpha1ClusterStream clusterStream) {
		return new V1OwnerReference().controller(true)
				.name(clusterStream.getMetadata().getName())
				.uid(clusterStream.getMetadata().getUid())
				.kind(clusterStream.getKind())
				.apiVersion(clusterStream.getApiVersion())
				.blockOwnerDeletion(true);
	}

	private void logFailureEvent(V1alpha1ClusterStream clusterStream, String namespace, String reason, Exception e) {
		String message = String.format("Failed to deploy Cluster Stream %s: %s", clusterStream.getMetadata().getName(), reason);
		LOG.error(message, e);
		eventRecorder.logEvent(
				EventRecorder.toObjectReference(clusterStream).namespace(namespace),
				null,
				ClusterStreamConfiguration.CLUSTER_STREAM_CONTROLLER_NAME,
				e.getClass().getName(),
				message + ": " + e.getMessage(),
				EventType.Warning);
	}

	private void setClusterStreamStatus(V1alpha1ClusterStream clusterStream, String type, String status, String reason,
			String storageAddress) {

		if (!hasConditionChanged(clusterStream, status, reason)) {
			return;
		}


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
							clusterStream.getMetadata().getName(),
							new V1Patch(patch), null, null, null, null),
					V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
					api.getApiClient());
		}
		catch (ApiException e) {
			LOG.error("Status API call failed: {}: {}, {}, with patch {}", e.getCode(), e.getMessage(), e.getResponseBody(), patch);
		}
	}

	private boolean hasConditionChanged(V1alpha1ClusterStream clusterStream, String newReadyStatus, String newStatusReason) {
		if (clusterStream.getStatus() == null || clusterStream.getStatus().getConditions() == null) {
			return true;
		}

		return !clusterStream.getStatus().getConditions().stream().allMatch(
				condition -> newReadyStatus.equalsIgnoreCase(condition.getStatus())
						&& newStatusReason.equalsIgnoreCase(condition.getReason()));
	}

}
