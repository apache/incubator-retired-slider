/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web;

import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.services.security.CertificateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 
 */
public class WebAppApiImpl implements WebAppApi {
  private static final Logger log = LoggerFactory.getLogger(WebAppApiImpl.class);

  protected final SliderClusterProtocol clusterProto;
  protected final StateAccessForProviders appState;
  protected final ProviderService provider;
  protected final CertificateManager certificateManager;
  private final RegistryOperations registryOperations;
  private final MetricsAndMonitoring metricsAndMonitoring;

  public WebAppApiImpl(SliderClusterProtocol clusterProto,
      StateAccessForProviders appState,
      ProviderService provider,
      CertificateManager certificateManager,
      RegistryOperations registryOperations,
      MetricsAndMonitoring metricsAndMonitoring) {
    this.registryOperations = registryOperations;
    checkNotNull(clusterProto);
    checkNotNull(appState);
    checkNotNull(provider);
    
    this.clusterProto = clusterProto;
    this.appState = appState;
    this.provider = provider;
    this.certificateManager = certificateManager;
    this.metricsAndMonitoring = metricsAndMonitoring;
  }

  @Override
  public StateAccessForProviders getAppState() {
    return appState;
  }

  @Override
  public ProviderService getProviderService() {
    return provider;
  }

  @Override
  public CertificateManager getCertificateManager() {
    return certificateManager;
  }

  @Override
  public SliderClusterProtocol getClusterProtocol() {
    return clusterProto;
  }
  
  @Override
  public Map<String,RoleStatus> getRoleStatusByName() {
    List<RoleStatus> roleStatuses = appState.cloneRoleStatusList();
    Map<String, RoleStatus> map =
        new TreeMap<String, RoleStatus>();
    for (RoleStatus status : roleStatuses) {
      map.put(status.getName(), status);
    }
    return map;
  }

  @Override
  public AgentRestOperations getAgentRestOperations() {
    return provider.getAgentRestOperations();
  }

  @Override
  public RegistryOperations getRegistryOperations() {
    return registryOperations;
  }

  @Override
  public MetricsAndMonitoring getMetricsAndMonitoring() {
    return metricsAndMonitoring;
  }
}
