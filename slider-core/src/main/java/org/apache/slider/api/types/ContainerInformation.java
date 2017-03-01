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

package org.apache.slider.api.types;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.hadoop.registry.client.binding.JsonSerDeser;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Serializable version of component instance data
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ContainerInformation {
  
  public String containerId;
  public String component;
  public String appVersion;
  public Boolean released;
  public int state;
  public Integer exitCode;
  public String diagnostics;
  public long createTime;
  public long startTime;
  public long completionTime;

  public String host;
  public String hostURL;
  public String placement;
  /**
   * What is the tail output from the executed process (or [] if not started
   * or the log cannot be picked up
   */
  public String[] output;
  public String logLink;
  @JsonIgnore
  public String logServerLogLink;

  public String getContainerId() {
    return containerId;
  }

  public String getComponent() {
    return component;
  }

  public String getAppVersion() {
    return appVersion;
  }

  public Boolean getReleased() {
    return released;
  }

  public int getState() {
    return state;
  }

  public Integer getExitCode() {
    return exitCode;
  }

  public String getDiagnostics() {
    return diagnostics;
  }

  public long getCreateTime() {
    return createTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getCompletionTime() {
    return completionTime;
  }

  public String getHost() {
    return host;
  }

  public String getHostURL() {
    return hostURL;
  }

  public String getPlacement() {
    return placement;
  }

  public String[] getOutput() {
    return output;
  }

  public String getLogLink() {
    return logLink;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((containerId == null) ? 0 : containerId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContainerInformation other = (ContainerInformation) obj;
    if (containerId == null) {
      if (other.containerId != null)
        return false;
    } else if (!containerId.equals(other.containerId))
      return false;
    return true;
  }

  @Override
  public String toString() {
    JsonSerDeser<ContainerInformation> serDeser =
        new JsonSerDeser<>(
            ContainerInformation.class);
    return serDeser.toString(this);
  }

  /**
   * Compare two containers by their ids.
   */
  public static class CompareById implements Comparator<ContainerInformation>,
      Serializable {
    @Override
    public int compare(ContainerInformation c1, ContainerInformation c2) {
      return c1.getContainerId().compareTo(c2.getContainerId());
    }
  }
}
