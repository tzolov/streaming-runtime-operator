package com.vmware.tanzu.streaming.runtime.uitil;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import io.kubernetes.client.PortForward;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental Service to allow programmatic port-forwarding for the purpose of IDE/Local testing.
 */
public class KubernetesPortForwarder implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesPortForwarder.class);

	private final ApiClient apiClient;
	private final String name;
	private final String namespace;
	private final int localPort;
	private final int targetPort;

	private boolean running = false;

	private PortForward.PortForwardResult portForwardResult;

	public KubernetesPortForwarder(ApiClient apiClient, String name, String namespace, int localPort, int targetPort) {
		this.apiClient = apiClient;
		this.name = name;
		this.namespace = namespace;
		this.localPort = localPort;
		this.targetPort = targetPort;
	}

	public void start() throws ApiException {

		running = true;
		PortForward pf = new PortForward(apiClient);
		try {
			portForwardResult = pf.forward(namespace, name, List.of(targetPort));
		}
		catch (IOException e) {
			LOG.error("PortForward failed!", e);
			throw new ApiException("PortForward failed because of: " + e.getMessage());
		}

		if (portForwardResult == null) {
			LOG.error("PortForward failed!");
			throw new ApiException("PortForward failed!");
		}

		// Start a port-forwarding thread
		Thread t = new Thread(() -> {
			while (running) {
				try (Socket sock = new ServerSocket(localPort).accept()) {
					LOG.info(String.format("Start [%s:%s] port-forwarding for (%s/%s)", localPort, targetPort, name, namespace));

					Thread copyLocalToTarget = copyAsync(sock.getInputStream(), portForwardResult.getOutboundStream(targetPort));
					Thread copyTargetToLocal = copyAsync(portForwardResult.getInputStream(targetPort), sock.getOutputStream());

					copyLocalToTarget.join();
					copyTargetToLocal.join();
				}
				catch (IOException | InterruptedException ex) {
					LOG.error(String.format("Failed the [%s:%s] port-forwarding for (%s/%s)",
							localPort, targetPort, name, namespace), ex);
				}
			}
			LOG.info(String.format("Exit the [%s:%s] port-forwarding for (%s/%s)", localPort, targetPort, name, namespace));
		});

		t.start();

//		try {
//			Thread.sleep(5000);
//		}
//		catch (InterruptedException e) {
//			// Do nothing
//		}
		try {
			t.join();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void shutdown() {
		running = false;
		try {
			//portForwardResult.getOutboundStream(targetPort).flush();
			portForwardResult.getOutboundStream(targetPort).close();
		}
		catch (IOException e) {
			LOG.warn("Shutdown error:", e);
		}
		try {
			portForwardResult.getInputStream(targetPort).close();
		}
		catch (IOException e) {
			LOG.warn("Shutdown error:", e);
		}
	}

	protected Thread copyAsync(InputStream in, OutputStream out) {
		Thread t = new Thread(() -> {
			try {
				IOUtils.copy(in, out);
			}
			catch (IOException ex) {
				LOG.warn("i/o streams copy problem: " + ex.getMessage());
			}
		});
		t.start();
		return t;
	}

	@Override
	public void close() throws IOException {
		shutdown();
	}
}
