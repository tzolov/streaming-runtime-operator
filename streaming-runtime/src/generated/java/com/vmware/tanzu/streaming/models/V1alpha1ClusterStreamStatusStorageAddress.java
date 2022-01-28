/*
 * Kubernetes
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: v1.19.11
 * 
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */


package com.vmware.tanzu.streaming.models;

import java.util.Objects;
import java.util.Arrays;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.vmware.tanzu.streaming.models.V1alpha1ClusterStreamStatusStorageAddressServers;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * V1alpha1ClusterStreamStatusStorageAddress
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2022-01-28T15:22:14.271Z[Etc/UTC]")
public class V1alpha1ClusterStreamStatusStorageAddress {
  public static final String SERIALIZED_NAME_SERVERS = "servers";
  @SerializedName(SERIALIZED_NAME_SERVERS)
  private Map<String, V1alpha1ClusterStreamStatusStorageAddressServers> servers = null;


  public V1alpha1ClusterStreamStatusStorageAddress servers(Map<String, V1alpha1ClusterStreamStatusStorageAddressServers> servers) {
    
    this.servers = servers;
    return this;
  }

  public V1alpha1ClusterStreamStatusStorageAddress putServersItem(String key, V1alpha1ClusterStreamStatusStorageAddressServers serversItem) {
    if (this.servers == null) {
      this.servers = new HashMap<>();
    }
    this.servers.put(key, serversItem);
    return this;
  }

   /**
   * Get servers
   * @return servers
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public Map<String, V1alpha1ClusterStreamStatusStorageAddressServers> getServers() {
    return servers;
  }


  public void setServers(Map<String, V1alpha1ClusterStreamStatusStorageAddressServers> servers) {
    this.servers = servers;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1ClusterStreamStatusStorageAddress v1alpha1ClusterStreamStatusStorageAddress = (V1alpha1ClusterStreamStatusStorageAddress) o;
    return Objects.equals(this.servers, v1alpha1ClusterStreamStatusStorageAddress.servers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(servers);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1ClusterStreamStatusStorageAddress {\n");
    sb.append("    servers: ").append(toIndentedString(servers)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

