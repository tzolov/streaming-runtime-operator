package com.vmware.tanzu.streaming.runtime.protocol;

import java.io.IOException;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;

public interface ProtocolDeploymentEditor {

	String getProtocolName();

	boolean create(V1OwnerReference ownerReference) throws IOException, ApiException;

	boolean isAllRunning(V1OwnerReference ownerReference) throws ApiException;

	String storageAddress(V1OwnerReference ownerReference)  throws ApiException;
}
