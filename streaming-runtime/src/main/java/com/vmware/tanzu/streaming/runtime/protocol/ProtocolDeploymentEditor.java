package com.vmware.tanzu.streaming.runtime.protocol;

import java.io.IOException;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;

public interface ProtocolDeploymentEditor {

	String getProtocolName();

	boolean createMissingServicesAndDeployments(V1OwnerReference ownerReference, String namespace) throws IOException, ApiException;

	boolean isAllRunning(V1OwnerReference ownerReference, String namespace);

	String storageAddress(V1OwnerReference ownerReference, String namespace);
}
