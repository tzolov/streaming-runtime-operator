package com.vmware.tanzu.streaming.runtime.protocol;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

@Component
public class RabbitMqDeploymentEditor implements ProtocolDeploymentEditor{

	private static final Logger LOG = LoggerFactory.getLogger(RabbitMqDeploymentEditor.class);

	private final CoreV1Api coreV1Api;
	private final AppsV1Api appsV1Api;
	private final ObjectMapper yamlMapper;

	private static final Resource rabbitmqService = toResource("classpath:manifests/rabbitmq/rabbitmq-svc.yaml");
	private static final Resource rabbitmqDeployment = toResource("classpath:manifests/rabbitmq/rabbitmq-deployment.yaml");

	public RabbitMqDeploymentEditor(CoreV1Api coreV1Api, AppsV1Api appsV1Api, ObjectMapper yamlMapper) {
		this.coreV1Api = coreV1Api;
		this.appsV1Api = appsV1Api;
		this.yamlMapper = yamlMapper;
	}

	@Override
	public String getProtocolName() {
		return "rabbitmq";
	}

	@Override
	public boolean create(V1OwnerReference ownerReference) throws IOException, ApiException {

		boolean changed = false;
		if (CollectionUtils.isEmpty(findServices("default", null,"app=rabbitmq"))) {
			this.createService(ownerReference, rabbitmqService, "default");
			changed = true;
		}

		if (CollectionUtils.isEmpty(findPods("default", null,"app=rabbitmq"))) {
			this.createDeployment(ownerReference, rabbitmqDeployment, "default", "rabbitmq:3-management");
			changed = true;
		}
		return changed;
	}

	@Override
	public boolean isAllRunning(V1OwnerReference ownerReference) throws ApiException {
		int size = findPods("default", "status.phase=Running", "app in (rabbitmq)").size();
		return size == 2;
	}

	@Override
	public String storageAddress(V1OwnerReference ownerReference) throws ApiException {
		return "TODO";
	}

	private List<V1Pod> findPods(String namesapce, String fieldSelector, String labelSelector) throws ApiException {
		return coreV1Api.listNamespacedPod(namesapce, null, null, null, fieldSelector,
				labelSelector, null, null, null, null, null).getItems();
	}

	private List<V1Service> findServices(String namesapce, String fieldSelector, String selector) throws ApiException {
		return coreV1Api.listNamespacedService(namesapce, null, null, null, fieldSelector,
				selector, null, null, null, null, null).getItems();
	}

	private V1Deployment createDeployment(V1OwnerReference ownerReference,
			Resource deploymentYaml, String appNamespace,
			String appImage) throws IOException, ApiException {
		LOG.debug("Creating deployment {}/{}", appNamespace, ownerReference.getName());
		V1Deployment body = yamlMapper.readValue(deploymentYaml.getInputStream(), V1Deployment.class);
		//body.getMetadata().setName(ownerReference.getName());
		body.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
		//body.getSpec().getSelector().getMatchLabels().put("app", ownerReference.getName());
		//body.getSpec().getTemplate().getMetadata().getLabels().put("app", ownerReference.getName());
//		body.getSpec().getTemplate().getSpec().getVolumes().get(0).getConfigMap().setName(ownerReference.getName());
//		body.getSpec().getTemplate().getSpec().getContainers().get(0).setImage(appImage);
		body.getSpec().getTemplate().getMetadata().getLabels().put("streaming-runtime", ownerReference.getName());


		return appsV1Api.createNamespacedDeployment(appNamespace, body, null, null, null);
	}

	private V1Service createService(V1OwnerReference ownerReference,
			Resource serviceYaml, String appNamespace) throws IOException, ApiException {
		LOG.debug("Creating service {}/{}", appNamespace, ownerReference.getName());
		V1Service body = yamlMapper.readValue(serviceYaml.getInputStream(), V1Service.class);
		//body.getMetadata().setName(ownerReference.getName());
		body.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
		//body.getSpec().getSelector().getMatchLabels().put("app", ownerReference.getName());
		body.getMetadata().getLabels().put("streaming-runtime", ownerReference.getName());

		return coreV1Api.createNamespacedService(appNamespace, body, null, null, null);
	}

	private static Resource toResource(String uri) {
		return new DefaultResourceLoader().getResource(uri);
	}
}
