/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.core.registry.info;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Service instance data to serialize with JSON.
 * 
 * The equality and hash codes are derived from the
 * service type and ID, which aren't final so that JSON marshalling
 * works. Do not change these fields if an instance is stored
 * in a map
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ServiceInstanceData implements Serializable {

  public String serviceType;
  public String id;
  public String description;
  public String yarnApplicationId;
  public long registrationTimeUTC;

  /**
   * Anything can go into the internal view, it's specific
   * to the provider
   */
  public RegistryView internalView = new RegistryView();

  /**
   * External view of the service: public information
   */
  public RegistryView externalView = new RegistryView();

  public ServiceInstanceData() {
  }

  public ServiceInstanceData(String id, String serviceType) {
    this.serviceType = serviceType;
    this.id = id;
  }

  /**
   * Instances are equal if they look after the same service type
   * and name
   * @param o other
   * @return true if id and type match
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ServiceInstanceData that = (ServiceInstanceData) o;

    if (!id.equals(that.id)) {
      return false;
    }
    if (!serviceType.equals(that.serviceType)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = serviceType.hashCode();
    result = 31 * result + id.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("ServiceInstanceData{");
    sb.append("id='").append(id).append('\'');
    sb.append(", serviceType='").append(serviceType).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /**
   * get the internal or external registry
   * @param external flag to indicate the external endpoints
   * @return a view -which may be null
   */
  public RegistryView getRegistryView(boolean external) {
    return external ? externalView : internalView;
  }

  /**
   * List the internal or external endpoints. This returns
   * an empty list if there are none registered
   * @param external flag to indicate the external endpoints
   * @return a map of published endpoints
   */
  public Map<String, RegisteredEndpoint> listEndpoints(boolean external) {
    RegistryView view = getRegistryView(external);
    if (view == null) {
      return new HashMap<String, RegisteredEndpoint>(0);
    }
    Map<String, RegisteredEndpoint> endpoints = view.endpoints;
    if (endpoints != null) {
      return endpoints;
    } else {
      return new HashMap<String, RegisteredEndpoint>(0);
    }
  }
  
}


