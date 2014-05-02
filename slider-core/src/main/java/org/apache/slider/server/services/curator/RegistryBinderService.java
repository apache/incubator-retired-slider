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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.ServiceType;
import org.apache.curator.x.discovery.UriSpec;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.persist.JsonSerDeser;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * YARN service for Curator service discovery; the discovery instance's
 * start/close methods are tied to the lifecycle of this service
 * @param <Payload> the payload of the operation
 */
public class RegistryBinderService<Payload> extends CuratorService {
  protected static final Logger log =
    LoggerFactory.getLogger(RegistryBinderService.class);

  private final ServiceDiscovery<Payload> discovery;

  private final Map<String, ServiceInstance<Payload>> entries =
    new HashMap<String, ServiceInstance<Payload>>();

  JsonSerDeser<CuratorServiceInstance<Payload>> deser =
    new JsonSerDeser<CuratorServiceInstance<Payload>>(
      CuratorServiceInstance.class);

  /**
   * Create an instance
   * @param curator. Again, does not need to be started
   * @param discovery discovery instance -not yet started
   */
  public RegistryBinderService(CuratorFramework curator,
                               String basePath,
                               ServiceDiscovery<Payload> discovery) {
    super("RegistryBinderService", curator, basePath);

    this.discovery =
      Preconditions.checkNotNull(discovery, "null discovery arg");
  }


  public ServiceDiscovery<Payload> getDiscovery() {
    return discovery;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    discovery.start();
  }

  @Override
  protected void serviceStop() throws Exception {
    closeCuratorComponent(discovery);
    super.serviceStop();
  }

  /**
   * register an instance -only valid once the service is started
   * @param id ID -must be unique
   * @param name name
   * @param url URL
   * @param payload payload (may be null)
   * @return the instance
   * @throws Exception on registration problems
   */
  public ServiceInstance<Payload> register(String name,
                                           String id,
                                           URL url,
                                           Payload payload) throws Exception {
    Preconditions.checkNotNull(id, "null `id` arg");
    Preconditions.checkNotNull(name, "null `name` arg");
    Preconditions.checkNotNull(url, "null `url` arg");
    Preconditions.checkState(isInState(STATE.STARTED), "Not started: " + this);

    if (lookup(id) != null) {
      throw new BadClusterStateException(
        "existing entry for service id %s name %s %s",
        id, name, url);
    }
    int port = url.getPort();
    if (port == 0) {
      throw new IOException("Port undefined in " + url);
    }
    UriSpec uriSpec = new UriSpec(url.toString());
    ServiceInstance<Payload> instance = builder()
      .name(name)
      .id(id)
      .payload(payload)
      .port(port)
      .serviceType(ServiceType.DYNAMIC)
      .uriSpec(uriSpec)
      .build();
    log.info("registering {}", instance.toString());
    discovery.registerService(instance);
    log.info("registration completed {}", instance.toString());
    synchronized (this) {
      entries.put(id, instance);
    }
    return instance;
  }

  /**
   * Get the registered instance by its ID
   * @param id ID
   * @return instance or null
   */
  public synchronized ServiceInstance<Payload> lookup(String id) {
    Preconditions.checkNotNull(id, "null `id` arg");
    return entries.get(id);
  }

  /**
   * Create a builder. This is already pre-prepared with address, registration
   * time and a (random) UUID
   * @return a builder
   * @throws IOException IO problems, including enumerating network ports
   */
  public ServiceInstanceBuilder<Payload> builder() throws
                                                   IOException {
    try {
      return ServiceInstance.builder();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }


  public List<String> instanceIDs(String servicename) throws Exception {
    List<String> instanceIds;

    try {
      instanceIds =
        getCurator().getChildren().forPath(pathForName(servicename));
    } catch (KeeperException.NoNodeException e) {
      instanceIds = Lists.newArrayList();
    }
    return instanceIds;
  }


  /**
   * Return a service instance POJO
   *
   * @param name name of the service
   * @param id ID of the instance
   * @return the instance or <code>null</code> if not found
   * @throws Exception errors
   */
  public CuratorServiceInstance<Payload> queryForInstance(String name, String id) throws
                                                                         Exception {
    String path = pathForInstance(name, id);
    try {
      byte[] bytes = getCurator().getData().forPath(path);
      return deser.fromBytes(bytes);
    } catch (KeeperException.NoNodeException ignore) {
      // ignore
    }
    return null;
  }

  
  
  /**
   * List all the instances
   * @param name name of the service
   * @return a list of instances and their payloads
   * @throws IOException any problem
   */
  public List<CuratorServiceInstance<Payload>> listInstances(String name) throws
    IOException {
    try {
      List<String> instanceIDs = instanceIDs(name);
      List<CuratorServiceInstance<Payload>> instances =
        new ArrayList<CuratorServiceInstance<Payload>>(
          instanceIDs.size());
      for (String instanceID : instanceIDs) {
        CuratorServiceInstance<Payload> instance =
          queryForInstance(name, instanceID);
        instances.add(instance);
      }
      return instances;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }


  public Collection<String> queryForNames() throws IOException {
    try {
      return getDiscovery().queryForNames();
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  
}
