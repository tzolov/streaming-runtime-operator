package com.vmware.tanzu.streaming.runtime;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.vmware.tanzu.streaming.apis.StreamingTanzuVmwareComV1alpha1Api;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.runtime.protocol.ProtocolDeploymentEditor;
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

import org.springframework.stereotype.Component;

@Component
public class StreamReconciler implements Reconciler {

	private static final Logger LOG = LoggerFactory.getLogger(StreamReconciler.class);
	private static final boolean REQUEUE = true;

	private final Lister<V1alpha1Stream> streamLister;
	private final Map<String, ProtocolDeploymentEditor> protocolDeploymentEditors;
	private final StreamingTanzuVmwareComV1alpha1Api api;

	public StreamReconciler(SharedIndexInformer<V1alpha1Stream> streamInformer,
			ProtocolDeploymentEditor[] protocolDeploymentEditors, StreamingTanzuVmwareComV1alpha1Api api) {

		this.api = api;
		this.streamLister = new Lister<>(streamInformer.getIndexer());
		this.protocolDeploymentEditors =
				Stream.of(protocolDeploymentEditors)
						.collect(Collectors.toMap(ProtocolDeploymentEditor::getProtocolName, Function.identity()));
	}

	@Override
	public Result reconcile(Request request) {

		V1alpha1Stream stream = this.streamLister.get(request.getName());

		final boolean toAdd = (stream.getMetadata() != null) && (stream.getMetadata().getGeneration() == null
				|| stream.getMetadata().getGeneration() == 1);

		final boolean toUpdate = (stream.getMetadata() != null) && (stream.getMetadata().getGeneration() != null
				&& stream.getMetadata().getGeneration() > 1
				&& stream.getMetadata().getDeletionTimestamp() == null);

		try {
			V1OwnerReference ownerReference = toOwnerReference(stream);
			//for (V1alpha1ClusterStreamSpecStorageServers server : clusterStream.getSpec().getStorage().getServers()) {
			//	ProtocolDeploymentEditor protocolDeploymentEditor = this.protocolDeploymentEditors.get(server.getProtocol());
			//	if (toAdd) {
			//		if (protocolDeploymentEditor.create(ownerReference)) {
			//			setClusterStreamStatus(clusterStream, "Ready", "false",
			//					"create-" + protocolDeploymentEditor.getProtocolName() + "-cluster",
			//					protocolDeploymentEditor.storageAddress(ownerReference));
			//		}
			//		else {
			//			if (protocolDeploymentEditor.isAllRunning(ownerReference)) {
			//				if (!clusterStream.getStatus().getConditions().stream().map(c -> c.getStatus()).allMatch(s -> s.equalsIgnoreCase("true"))) {
			//					setClusterStreamStatus(clusterStream, "Ready", "true",
			//							"running-" + protocolDeploymentEditor.getProtocolName() + "-cluster",
			//							protocolDeploymentEditor.storageAddress(ownerReference));
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
			logFailureEvent(stream, e.getMessage(), e);
			return new Result(REQUEUE);
		}
		return new Result(!REQUEUE);
	}

	private V1OwnerReference toOwnerReference(V1alpha1Stream stream) {
		return new V1OwnerReference().controller(true)
				.name(stream.getMetadata().getName())
				.uid(stream.getMetadata().getUid())
				.kind(stream.getKind())
				.apiVersion(stream.getApiVersion())
				.blockOwnerDeletion(true);
	}

	private void logFailureEvent(V1alpha1Stream stream, String reason, Exception e) {
		e.printStackTrace();
		String message = String.format("Failed to deploy Stream %s: %s", stream.getMetadata().getName(), reason);
		LOG.error(message);
		//eventRecorder.logEvent(
		//		toOwnerReference(stream).namespace(adoptionCenterNamespace),
		//		null,
		//		e.getClass().getName(),
		//		message + ": " + e.getMessage(),
		//		EventType.Warning);
	}

	public void setStreamStatus(V1alpha1Stream stream, String type, String status, String reason,
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
			PatchUtils.patch(V1alpha1Stream.class,
					() -> api.patchNamespacedStreamCall(
							stream.getMetadata().getName(),
							"TODO NAMESPACE",
							new V1Patch(patch), null, null, null, null),
					V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
					api.getApiClient());
		}
		catch (ApiException e) {
			LOG.error("Status API call failed: {}: {}, {}, with patch {}", e.getCode(), e.getMessage(), e.getResponseBody(), patch);
		}
	}
}
