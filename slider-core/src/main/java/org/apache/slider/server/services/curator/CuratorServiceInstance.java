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

package org.apache.slider.server.services.curator;

import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CuratorServiceInstance<T> {

  public String name;
  public String id;
  public String address;
  public Integer port;
  public Integer sslPort;
  public T payload;
  public long registrationTimeUTC;
  public ServiceType serviceType;
  public CuratorUriSpec uriSpec;

  public CuratorServiceInstance() {
  }

  public CuratorServiceInstance(ServiceInstance<T> si) {
    this.name = si.getName();
    this.id = si.getId();
    this.address = si.getAddress();
    this.port = si.getPort();
    this.sslPort = si.getSslPort();
    this.payload = si.getPayload();
    this.registrationTimeUTC = si.getRegistrationTimeUTC();
    this.serviceType = si.getServiceType();
    this.uriSpec = new CuratorUriSpec();
    this.uriSpec.addAll(si.getUriSpec().getParts());
  }


  @Override
  public String toString() {
    final StringBuilder sb =
      new StringBuilder("CuratorServiceInstance{");
    sb.append("name='").append(name).append('\'');
    sb.append(", id='").append(id).append('\'');
    sb.append(", address='").append(address).append('\'');
    sb.append(", port=").append(port);
    sb.append(", sslPort=").append(sslPort);
    sb.append(", payload=").append(payload);
    sb.append(", registrationTimeUTC=").append(registrationTimeUTC);
    sb.append(", serviceType=").append(serviceType);
    sb.append(", uriSpec=").append(uriSpec);
    sb.append('}');
    return sb.toString();
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
  public T getPayload() {
    return payload;
  }
}
