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
import com.vmware.tanzu.streaming.models.V1alpha1ProcessorSpecTemplateSpecEnv;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * V1alpha1ProcessorSpecTemplateSpecContainers
 */
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", date = "2022-01-25T08:52:37.754Z[Etc/UTC]")
public class V1alpha1ProcessorSpecTemplateSpecContainers {
  public static final String SERIALIZED_NAME_ENV = "env";
  @SerializedName(SERIALIZED_NAME_ENV)
  private List<V1alpha1ProcessorSpecTemplateSpecEnv> env = null;

  public static final String SERIALIZED_NAME_IMAGE = "image";
  @SerializedName(SERIALIZED_NAME_IMAGE)
  private String image;

  public static final String SERIALIZED_NAME_NAME = "name";
  @SerializedName(SERIALIZED_NAME_NAME)
  private String name;


  public V1alpha1ProcessorSpecTemplateSpecContainers env(List<V1alpha1ProcessorSpecTemplateSpecEnv> env) {
    
    this.env = env;
    return this;
  }

  public V1alpha1ProcessorSpecTemplateSpecContainers addEnvItem(V1alpha1ProcessorSpecTemplateSpecEnv envItem) {
    if (this.env == null) {
      this.env = new ArrayList<>();
    }
    this.env.add(envItem);
    return this;
  }

   /**
   * Get env
   * @return env
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public List<V1alpha1ProcessorSpecTemplateSpecEnv> getEnv() {
    return env;
  }


  public void setEnv(List<V1alpha1ProcessorSpecTemplateSpecEnv> env) {
    this.env = env;
  }


  public V1alpha1ProcessorSpecTemplateSpecContainers image(String image) {
    
    this.image = image;
    return this;
  }

   /**
   * Get image
   * @return image
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getImage() {
    return image;
  }


  public void setImage(String image) {
    this.image = image;
  }


  public V1alpha1ProcessorSpecTemplateSpecContainers name(String name) {
    
    this.name = name;
    return this;
  }

   /**
   * Get name
   * @return name
  **/
  @javax.annotation.Nullable
  @ApiModelProperty(value = "")

  public String getName() {
    return name;
  }


  public void setName(String name) {
    this.name = name;
  }


  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    V1alpha1ProcessorSpecTemplateSpecContainers v1alpha1ProcessorSpecTemplateSpecContainers = (V1alpha1ProcessorSpecTemplateSpecContainers) o;
    return Objects.equals(this.env, v1alpha1ProcessorSpecTemplateSpecContainers.env) &&
        Objects.equals(this.image, v1alpha1ProcessorSpecTemplateSpecContainers.image) &&
        Objects.equals(this.name, v1alpha1ProcessorSpecTemplateSpecContainers.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(env, image, name);
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class V1alpha1ProcessorSpecTemplateSpecContainers {\n");
    sb.append("    env: ").append(toIndentedString(env)).append("\n");
    sb.append("    image: ").append(toIndentedString(image)).append("\n");
    sb.append("    name: ").append(toIndentedString(name)).append("\n");
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

