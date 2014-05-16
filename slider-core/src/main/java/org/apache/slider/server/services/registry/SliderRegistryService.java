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

package org.apache.slider.server.services.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.server.services.curator.CuratorServiceInstance;
import org.apache.slider.server.services.curator.RegistryBinderService;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * This is the registry service, which tries to hide exactly how the
 * registry is implemented
 */

public class SliderRegistryService
    extends RegistryBinderService<ServiceInstanceData>
    implements RegistryViewForProviders {
  
  private ServiceInstanceData selfRegistration;

  public SliderRegistryService(CuratorFramework curator,
      String basePath,
      ServiceDiscovery<ServiceInstanceData> discovery) {
    super(curator, basePath, discovery);
  }


  @Override
  public List<ServiceInstanceData> listInstancesByType(String serviceType) throws
      IOException {
    List<CuratorServiceInstance<ServiceInstanceData>> services =
        listInstances(serviceType);
    List<ServiceInstanceData> payloads = new ArrayList<>(services.size());
    for (CuratorServiceInstance<ServiceInstanceData> instance : services) {
      payloads.add(instance.payload);
    }
    return payloads;
  }

  @Override
  public ServiceInstanceData getSelfRegistration() {
    return selfRegistration;
  }

  private void setSelfRegistration(ServiceInstanceData selfRegistration) {
    this.selfRegistration = selfRegistration;
  }

  /**
   * register an instance -only valid once the service is started.
   * This sets the selfRegistration field
   * @param id ID -must be unique
   * @param name name
   * @param url URL
   * @param payload payload (may be null)
   * @return the instance
   * @throws IOException on registration problems
   */
  public void registerSelf(String name,
      String id,
      URL url,
      ServiceInstanceData instanceData) throws IOException {
    try {
      register(name, id, url, instanceData);
      setSelfRegistration(instanceData);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
