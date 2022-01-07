package com.vmware.tanzu.streaming.runtime.throwaway;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import com.vmware.tanzu.streaming.runtime.uitil.KubernetesPortForwarder;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;

public class Main3 {


	public static void main(String[] args) throws IOException, ApiException, KubectlException, InterruptedException {
		ApiClient apiClient = Config.defaultClient();
		CoreV1Api coreApi = new CoreV1Api(apiClient);

		// FInd the Rabbitmq Server Pod by name
		String rmqPodName = "hello-world";
		String rmqPodNamespace = "default";
		V1PodList pod = coreApi.listNamespacedPod(rmqPodNamespace,
				null, null, null, null,
				"app.kubernetes.io/name in (" + rmqPodName + "),app.kubernetes.io/component in (rabbitmq)",
				null, null, null, null, null);

		String rmqServerInstanceName = pod.getItems().get(0).getMetadata().getName();
		String rmqServerInstanceNamespace = pod.getItems().get(0).getMetadata().getNamespace();

		// Start port forwarding
		KubernetesPortForwarder portForwardUtil = new KubernetesPortForwarder(apiClient,
				rmqServerInstanceName, rmqServerInstanceNamespace, 5672, 5672);
		portForwardUtil.start();

//		try {
//			String pfResp1 = coreApi.connectPostNamespacedPodPortforward(rmqServerInstanceName, rmqServerInstanceNamespace, 5672);
//			System.out.println(pfResp1);
//		}
//		catch (ApiException e) {
//			e.printStackTrace();
//		}

//		KubectlPortForward portForward = Kubectl.portforward().apiClient(apiClient)
//				.name(rmqServerInstanceName) // hello-world-server-0
//				.namespace(rmqServerInstanceNamespace) // default
//				.ports(5672, 5672);
//
//		Thread tt = new Thread(() -> {
//			try {
//				System.out.println("Start PF!");
//				portForward.execute();
//			}
//			catch (KubectlException e) {
//				e.printStackTrace();
//			}
//
//			System.out.println("End execute!");
//		});
//
//		tt.start();


	//	Thread.sleep(100);

		System.out.println("Reachable: " + isAddressReachable("127.0.0.1", 5672, 5000));

//		portForward.shutdown();
//		tt.interrupt();

		portForwardUtil.shutdown();

		apiClient.getHttpClient().connectionPool().evictAll();
	}

	public static boolean isAddressReachable(String address, int port, int timeout) {
		try (Socket testSocket = new Socket()) {
			testSocket.connect(new InetSocketAddress(address, port), timeout);
			return true;
		}
		catch (IOException exception) {
			return false;
		}
	}

}

