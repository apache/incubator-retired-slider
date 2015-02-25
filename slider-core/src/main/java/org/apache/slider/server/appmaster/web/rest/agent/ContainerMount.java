/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.server.appmaster.web.rest.agent;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a docker container mount
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ContainerMount {
  protected static final Logger
      log = LoggerFactory.getLogger(ContainerMount.class);


  // TODO: Rename
  private String containerMount;
  private String hostMount;

  public ContainerMount() {
  }

  public String getContainerMount() {
    return containerMount;
  }

  public void setContainerMount(String containerMount) {
    this.containerMount = containerMount;
  }

  public String getHostMount() {
    return hostMount;
  }

  public void setHostMount(String hostMount) {
    this.hostMount = hostMount;
  }
}
