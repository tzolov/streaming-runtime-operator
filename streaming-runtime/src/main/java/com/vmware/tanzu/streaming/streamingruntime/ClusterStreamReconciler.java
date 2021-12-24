package com.vmware.tanzu.streaming.streamingruntime;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import com.vmware.tanzu.streaming.apis.StreamingTanzuVmwareComV1alpha1Api;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStream;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamSpecStorageServers;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.PatchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ClusterStreamReconciler implements Reconciler {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterStreamReconciler.class);
	private static final boolean REQUEUE = true;

	private final Lister<V1alpha1ClusterStream> clusterStreamLister;
	private final KafkaDeploymentEditor kafkaDeploymentEditor;
	private final StreamingTanzuVmwareComV1alpha1Api api;

	public ClusterStreamReconciler(SharedIndexInformer<V1alpha1ClusterStream> clusterStreamInformer,
			@Autowired KafkaDeploymentEditor kafkaDeploymentEditor, StreamingTanzuVmwareComV1alpha1Api api) {
		this.clusterStreamLister = new Lister<>(clusterStreamInformer.getIndexer());
		this.kafkaDeploymentEditor = kafkaDeploymentEditor;
		this.api = api;
	}

	@Override
	public Result reconcile(Request request) {

		V1alpha1ClusterStream clusterStream = this.clusterStreamLister.get(request.getName());

		final boolean toAdd = clusterStream.getMetadata().getGeneration() == null
				|| clusterStream.getMetadata().getGeneration() == 1;

		final boolean toUpdate = clusterStream.getMetadata().getGeneration() != null
				&& clusterStream.getMetadata().getGeneration() > 1
				&& clusterStream.getMetadata().getDeletionTimestamp() == null;

		try {
			V1OwnerReference ownerReference = toOwnerReference(clusterStream);
			for (V1alpha1ClusterStreamSpecStorageServers server : clusterStream.getSpec().getStorage().getServers()) {
				if ("kafka".equalsIgnoreCase(server.getProtocol())) {
					if (toAdd) {
						if (kafkaDeploymentEditor.createKafka(ownerReference)) {
							setClusterStreamStatus(clusterStream, "Ready", "false", "CreatingKafkaCluster");
						} else {
							if (kafkaDeploymentEditor.isAllRunning(ownerReference)) {
								if (!clusterStream.getStatus().getConditions().stream().map(c -> c.getStatus()).allMatch(s -> s.equalsIgnoreCase("true"))) {
									setClusterStreamStatus(clusterStream, "Ready", "true", "RunningKafkaCluster");
								}
							} else {
								return new Result(REQUEUE, Duration.of(30, ChronoUnit.SECONDS));
							}
						}
					}
					else if (toUpdate) {
						//TODO
						System.out.println("UPDATE");
					}
				}
				else if ("rabbit".equalsIgnoreCase(server.getProtocol())) {
					// TODO
				}
			}
		}
		//catch (ApiException e) {
		//	if (e.getCode() == 409) {
		//		LOG.info("Required subresource is already present, skip creation.");
		//		return new Result(false);
		//	}
		//	logFailureEvent(clusterStream, e.getCode() + " - " + e.getResponseBody(), e);
		//	return new Result(true);
		//}
		catch (Exception e) {
			logFailureEvent(clusterStream, e.getMessage(), e);
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

	private void logFailureEvent(V1alpha1ClusterStream clusterStream, String reason, Exception e) {
		e.printStackTrace();
		String message = String.format("Failed to deploy Cluster Stream %s: %s", clusterStream.getMetadata().getName(), reason);
		LOG.error(message);
		//eventRecorder.logEvent(
		//		toOwnerReference(clusterStream).namespace(adoptionCenterNamespace),
		//		null,
		//		e.getClass().getName(),
		//		message + ": " + e.getMessage(),
		//		EventType.Warning);
	}

	public void setClusterStreamStatus(V1alpha1ClusterStream clusterStream, String type, String status, String reason) {

		String patch = String.format("" +
						"{\"status\": " +
						"  {\"conditions\": " +
						"    [{ \"type\": \"%s\", \"status\": \"%s\", \"lastTransitionTime\": \"%s\", \"reason\": \"%s\"}]" +
						"  }" +
						"}",
				type, status, ZonedDateTime.now(ZoneOffset.UTC), reason);
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
}
