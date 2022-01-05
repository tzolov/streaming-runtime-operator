package com.vmware.tanzu.streaming.runtime.config;

import com.vmware.tanzu.streaming.models.V1alpha1ClusterStream;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamList;
import com.vmware.tanzu.streaming.runtime.ClusterStreamReconciler;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.DefaultControllerWatch;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.workqueue.WorkQueue;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class ClusterStreamConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterStreamConfiguration.class);

	public static final String CLUSTER_STREAM_CONTROLLER_NAME = "ClusterStreamController";
	private static final int WORKER_COUNT = 4;

	@Bean
	public SharedIndexInformer<V1alpha1ClusterStream> clusterStreamsInformer(
			ApiClient apiClient, SharedInformerFactory sharedInformerFactory) {
		GenericKubernetesApi<V1alpha1ClusterStream, V1alpha1ClusterStreamList> genericApi =
				new GenericKubernetesApi<>(
						V1alpha1ClusterStream.class,
						V1alpha1ClusterStreamList.class,
						"streaming.tanzu.vmware.com",
						"v1alpha1",
						"clusterstreams",
						apiClient);
		return sharedInformerFactory.sharedIndexInformerFor(genericApi, V1alpha1ClusterStream.class, 0);
	}

	@Bean
	@Qualifier("clusterStreamController")
	Controller clusterStreamController(SharedInformerFactory factory, ClusterStreamReconciler clusterStreamReconciler,
			SharedIndexInformer<V1alpha1ClusterStream> clusterStreamInformer) {
		return ControllerBuilder.defaultBuilder(factory)
				.watch(this::createClusterStreamControllerWatch)
				.withReconciler(clusterStreamReconciler)
				.withName(CLUSTER_STREAM_CONTROLLER_NAME)
				.withWorkerCount(WORKER_COUNT)
				.withReadyFunc(clusterStreamInformer::hasSynced)
				.build();
	}

	private DefaultControllerWatch<V1alpha1ClusterStream> createClusterStreamControllerWatch(WorkQueue<Request> workQueue) {
		return ControllerBuilder.controllerWatchBuilder(V1alpha1ClusterStream.class, workQueue)
				.withOnAddFilter(clusterStream -> {
					LOG.info(String.format("[%s] Event: Add ClusterStream '%s'",
							CLUSTER_STREAM_CONTROLLER_NAME, clusterStream.getMetadata().getName()));
					return true;
				})
				.withOnUpdateFilter((oldClusterStream, newClusterStream) -> {
					LOG.info(String.format(
							"[%s] Event: Update ClusterStream '%s' to %s'",
							CLUSTER_STREAM_CONTROLLER_NAME, oldClusterStream.getMetadata().getName(),
							newClusterStream.getMetadata().getName()));
					return true;
				})
				.withOnDeleteFilter((deletedClusterStream, deletedFinalStateUnknown) -> {
					LOG.info(String.format("[%s] Event: Delete ClusterStream '%s'",
							CLUSTER_STREAM_CONTROLLER_NAME, deletedClusterStream.getMetadata().getName()));
					return false;
				})
				.build();
	}

	@Bean
	public Lister<V1ConfigMap> configMapLister(
			@Value("${streaming-runtime.namespace}") String streamingRuntimeNamespace,
			ApiClient apiClient,
			SharedInformerFactory sharedInformerFactory) {
		GenericKubernetesApi<V1ConfigMap, V1ConfigMapList> genericApi =
				new GenericKubernetesApi<>(V1ConfigMap.class, V1ConfigMapList.class, "", "v1", "configmaps", apiClient);
		SharedIndexInformer<V1ConfigMap> informer = sharedInformerFactory.sharedIndexInformerFor
				(genericApi, V1ConfigMap.class, 60 * 1000L, streamingRuntimeNamespace);
		return new Lister<>(informer.getIndexer());
	}
}
