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
package org.apache.slider.api.types;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.slider.api.SliderExitReason;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ApplicationDiagnostics {
  private static final Logger logger = LoggerFactory
      .getLogger(ApplicationDiagnostics.class);

  @JsonIgnore
  private Map<String, ContainerInformation> containersMap = new HashMap<>();
  private FinalApplicationStatus finalStatus;
  private String finalMessage;
  private SliderExitReason exitReason;
  private Set<ContainerInformation> containers = new HashSet<>();
  private Set<String> recentFailedContainers = new HashSet<>();

  public Collection<ContainerInformation> getContainers() {
    return Collections.unmodifiableCollection(containers);
  }

  public ContainerInformation getContainer(String containerId) {
    return containersMap.get(containerId);
  }

  public void addContainer(ContainerInformation container) {
    if (container == null) {
      return;
    }
    containersMap.put(container.containerId, container);
    containers.add(container);
  }

  public Collection<String> getRecentFailedContainers() {
    return Collections.unmodifiableCollection(recentFailedContainers);
  }

  public void setRecentFailedContainers(Collection<String> containerIds) {
    if (containerIds != null) {
      recentFailedContainers = new HashSet<>(containerIds);
    }
  }

  public void addRecentFailedContainer(String containerId) {
    if (containerId == null) {
      return;
    }
    recentFailedContainers.add(containerId);
  }

  public FinalApplicationStatus getFinalStatus() {
    return finalStatus;
  }

  public void setFinalStatus(FinalApplicationStatus finalStatus) {
    this.finalStatus = finalStatus;
  }

  public String getFinalMessage() {
    return finalMessage;
  }

  public void setFinalMessage(String finalMessage) {
    this.finalMessage = finalMessage;
  }

  public SliderExitReason getExitReason() {
    return exitReason;
  }

  public void setExitReason(SliderExitReason exitReason) {
    this.exitReason = exitReason;
  }

  @Override
  public String toString() {
    try {
      return toJsonString();
    } catch (Exception e) {
      logger.debug("Failed to convert ApplicationDiagnostics to JSON ", e);
      return super.toString();
    }
  }

  public String toJsonString()
      throws IOException, JsonGenerationException, JsonMappingException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    return mapper.writeValueAsString(this);
  }

  public static ApplicationDiagnostics fromJson(String json)
      throws IOException, JsonParseException, JsonMappingException {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, ApplicationDiagnostics.class);
    } catch (IOException e) {
      logger.error("Exception while parsing json : " + e + "\n" + json, e);
      throw e;
    }
  }

}
