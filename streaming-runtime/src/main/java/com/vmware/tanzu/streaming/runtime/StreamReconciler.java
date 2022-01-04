package com.vmware.tanzu.streaming.runtime;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.MissingResourceException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vmware.tanzu.streaming.apis.StreamingTanzuVmwareComV1alpha1Api;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStream;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamList;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamStatusStorageAddressServers;
import com.vmware.tanzu.streaming.models.V1alpha1Stream;
import com.vmware.tanzu.streaming.runtime.config.StreamConfiguration;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.extended.event.EventType;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.util.PatchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class StreamReconciler implements Reconciler {

	private static final Logger LOG = LoggerFactory.getLogger(StreamReconciler.class);
	private static final boolean REQUEUE = true;
	private static final String FINALIZER_STRING = "finalizer.streams.streaming.tanzu.vmware.com";

	private final Lister<V1alpha1Stream> streamLister;
	private final EventRecorder eventRecorder;
	private final ConfigMapUpdater configMapUpdater;
	private final StreamingTanzuVmwareComV1alpha1Api api;

	public StreamReconciler(SharedIndexInformer<V1alpha1Stream> streamInformer,
			StreamingTanzuVmwareComV1alpha1Api api, EventRecorder eventRecorder, ConfigMapUpdater configMapUpdater) {
		this.api = api;
		this.streamLister = new Lister<>(streamInformer.getIndexer());
		this.eventRecorder = eventRecorder;
		this.configMapUpdater = configMapUpdater;
	}

	@Override
	public Result reconcile(Request request) {

		String streamName = request.getName();
		String streamNamespace = request.getNamespace();
		V1alpha1Stream stream = this.streamLister.namespace(streamNamespace).get(streamName);

		final boolean toAdd = (stream.getMetadata() != null) && (stream.getMetadata().getGeneration() == null
				|| stream.getMetadata().getGeneration() == 1) && (stream.getStatus() == null);

		final boolean toUpdate = (stream.getMetadata() != null) && (stream.getMetadata().getGeneration() != null
				&& stream.getMetadata().getGeneration() > 1
				&& stream.getMetadata().getDeletionTimestamp() == null);

		final boolean toDelete = stream.getMetadata().getDeletionTimestamp() != null;

		String clusterStreamName = stream.getSpec().getStorage().getClusterStream();
		if (!configMapUpdater.configMapExists(clusterStreamName)) {
			logFailureEvent(stream, "find config map", "The targeted adoption center may not be ready,",
					new MissingResourceException("ConfigMap not found", V1ConfigMap.class.getName(), V1ConfigMap.class.getName()));
			//TODO catStatusEditor.setCatStatus(cat, "Ready", "False", "ConfigMapNotFound");
			setStreamStatus(stream, "Ready", "false", "ConfigMapNotFound", null);
			return new Result(REQUEUE);
		}

		try {
			if (toAdd) {
				V1alpha1ClusterStream clusterStream = findClusterStream(clusterStreamName);
				if (clusterStream == null) {
					throw new ApiException("Stream " + streamName + " without ClusterStream: " + clusterStreamName);
				}

				if (!clusterStream.getStatus().getConditions().stream().map(c -> c.getStatus()).allMatch(s -> s.equalsIgnoreCase("true"))) {
					ApiException e = new ApiException("Stream " + streamName + " with not completely deployed ClusterStream: " + clusterStreamName);
					logFailureEvent(stream, "", e.getCode() + " - " + e.getResponseBody(), e);
					return new Result(REQUEUE, Duration.of(30, ChronoUnit.SECONDS));
				}

				V1alpha1ClusterStreamStatusStorageAddressServers server
						= clusterStream.getStatus().getStorageAddress().getServers().get("production");
				if (!server.getProtocol().equalsIgnoreCase(stream.getSpec().getProtocol())) {
					throw new ApiException("Stream (" + streamName + " ) protocol (" + stream.getSpec().getProtocol() +
							") doesn't match the ClusterStream: " + clusterStreamName + " protocol:" + server.getProtocol());
				}
				addFinalizerIfNotFound(stream);
				setStreamStatus(stream, "Ready", "false", "CreateStream", null);
				// Update ConfigMap
				ConfigMapUpdater.StreamsProperties sp = new ConfigMapUpdater.StreamsProperties();
				sp.setStreams(new ArrayList<>());
				ConfigMapUpdater.Stream str = new ConfigMapUpdater.Stream();
				str.setName(streamName);
				sp.getStreams().add(str);
				configMapUpdater.updateConfigMap(clusterStreamName, sp);

				// Update status
				String storageAddressUpdate = "\"storageAddress\": " +
						new ObjectMapper().writeValueAsString(clusterStream.getStatus().getStorageAddress());
				setStreamStatus(stream, "Ready", "true", "ConfiguredStream", storageAddressUpdate);
			}
			else if (toUpdate) {
				addFinalizerIfNotFound(stream);
				// TODO update config map
				setStreamStatus(stream, "Ready", "false", "UpdateStream", null);
			}
			else if (toDelete) {
				configMapUpdater.removeStream(streamName, clusterStreamName);
				removeFinalizer(stream);
				setStreamStatus(stream, "Ready", "false", "DeleteStream", null);
			}
			else {
				LOG.error("Illegal state: received a request {} with nothing to do", request);
			}
		}
		catch (ApiException e) {
			if (e.getCode() == 409) {
				LOG.info("Required subresource is already present, skip creation.");
				return new Result(!REQUEUE);
			}
			logFailureEvent(stream, "", e.getCode() + " - " + e.getResponseBody(), e);
			return new Result(REQUEUE);
		}
		catch (Exception e) {
			logFailureEvent(stream, e.getMessage(), "", e);
			return new Result(REQUEUE);
		}
		return new Result(!REQUEUE);
	}

	//private V1OwnerReference toOwnerReference(V1alpha1Stream stream) {
	//	return new V1OwnerReference().controller(true)
	//			.name(stream.getMetadata().getName())
	//			.uid(stream.getMetadata().getUid())
	//			.kind(stream.getKind())
	//			.apiVersion(stream.getApiVersion())
	//			.blockOwnerDeletion(true);
	//}

	private V1alpha1ClusterStream findClusterStream(String clusterStreamName) throws ApiException {
		String fieldSelector = "metadata.name=" + clusterStreamName;

		V1alpha1ClusterStreamList clusterStreamList = api.listClusterStream(
				null, null, null, fieldSelector, null, null,
				null, null, null, false
		);
		// should only be one?
		return clusterStreamList.getItems().size() > 0 ? clusterStreamList.getItems().get(0) : null;
	}

	private void logFailureEvent(V1alpha1Stream stream, String reason, String errorBody, Exception e) {
		String message = String.format("Failed to %s for Stream %s/%s: %s",
				reason, stream.getMetadata().getNamespace(), stream.getMetadata().getName(), errorBody);
		LOG.error(message);
		eventRecorder.logEvent(
				EventRecorder.toObjectReference(stream),
				null,
				StreamConfiguration.STREAM_CONTROLLER_NAME,
				e.getClass().getName(),
				message + ": " + e.getMessage(),
				EventType.Warning);
	}

	private void setStreamStatus(V1alpha1Stream stream, String type, String status, String reason,
			String storageAddress) {

		if (StringUtils.hasText(storageAddress)) {
			storageAddress = "," + storageAddress;
		}
		else {
			storageAddress = "";
		}

		String patch = String.format("" +
						"{\"status\": " +
						"  {\"conditions\": " +
						"      [{ \"type\": \"%s\", \"status\": \"%s\", \"lastTransitionTime\": \"%s\", \"reason\": \"%s\"}]" +
						"     %s" +
						"  }" +
						"}",
				type, status, ZonedDateTime.now(ZoneOffset.UTC), reason, storageAddress);
		try {
			PatchUtils.patch(
					V1alpha1Stream.class,
					() -> api.patchNamespacedStreamStatusCall(
							stream.getMetadata().getName(),
							stream.getMetadata().getNamespace(),
							new V1Patch(patch),
							null, null, null, null),
					V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH,
					api.getApiClient());

		}
		catch (ApiException e) {
			LOG.error("Status API call failed: {}: {}, {}, with patch {}", e.getCode(), e.getMessage(), e.getResponseBody(), patch);
		}
	}

	private void addFinalizerIfNotFound(V1alpha1Stream stream) throws ApiException {
		LOG.debug("Checking for existing finalizers");
		boolean notFound = stream.getMetadata().getFinalizers() == null || stream.getMetadata().getFinalizers().isEmpty();
		if (notFound) {
			LOG.debug("Finalizers not found, adding one");
			streamPatch(stream, "{\"metadata\":{\"finalizers\":[\"" + FINALIZER_STRING + "\"]}}",
					V1Patch.PATCH_FORMAT_JSON_MERGE_PATCH);
		}
	}

	private V1alpha1Stream removeFinalizer(V1alpha1Stream stream) throws ApiException {
		// Currently, we don't have other finalizers so for now we just recklessly remove all finalizers.
		return streamPatch(stream, "[{\"op\": \"remove\", \"path\": \"/metadata/finalizers\"}]",
				V1Patch.PATCH_FORMAT_JSON_PATCH);
	}

	// NOTE: The api.patchNamespacedStreamCall(...) won't patch Stream's status! For this use the
	// api.patchNamespacedStreamStatusCall(...) install
	private V1alpha1Stream streamPatch(V1alpha1Stream stream, String jsonPatch, String patchFormat) throws ApiException {
		return PatchUtils.patch(
				V1alpha1Stream.class,
				() -> api.patchNamespacedStreamCall(
						stream.getMetadata().getName(),
						stream.getMetadata().getNamespace(),
						new V1Patch(jsonPatch),
						null, null, null, null),
				patchFormat,
				api.getApiClient());
	}
}
