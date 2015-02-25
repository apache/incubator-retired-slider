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

package org.apache.slider.providers.agent.application.metadata.json;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the metadata associated with the application.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class Component {
  protected static final Logger
      log = LoggerFactory.getLogger(Component.class);

  private String name;
  private String compExports;
  private String category = "MASTER";
  private String publishConfig;
  private int minInstanceCount = 1;
  private int maxInstanceCount = Integer.MAX_VALUE;
  private Boolean autoStartOnFailure = false;
  private String appExports;
  private CommandScript commandScript;
  private List<ComponentCommand> commands = new ArrayList<ComponentCommand>();

  public Component() {
  }

  @JsonProperty("commands")
  public List<ComponentCommand> getCommands() {
    return this.commands;
  }

  public CommandScript getCommandScript() {
    return commandScript;
  }

  public void setCommandScript(CommandScript commandScript) {
    this.commandScript = commandScript;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getCompExports() {
    return compExports;
  }

  public void setCompExports(String compExports) {
    this.compExports = compExports;
  }

  public String getCategory() {
    return category;
  }

  public void setCategory(String category) {
    this.category = category;
  }

  public String getPublishConfig() {
    return publishConfig;
  }

  public void setPublishConfig(String publishConfig) {
    this.publishConfig = publishConfig;
  }

  public int getMinInstanceCount() {
    return minInstanceCount;
  }

  public void setMinInstanceCount(int minInstanceCount) {
    this.minInstanceCount = minInstanceCount;
  }

  public int getMaxInstanceCount() {
    return maxInstanceCount;
  }

  public void setMaxInstanceCount(int maxInstanceCount) {
    this.maxInstanceCount = maxInstanceCount;
  }

  public Boolean getAutoStartOnFailure() {
    return autoStartOnFailure;
  }

  public void setAutoStartOnFailure(Boolean autoStartOnFailure) {
    this.autoStartOnFailure = autoStartOnFailure;
  }

  public String getAppExports() {
    return appExports;
  }

  public void setAppExports(String appExports) {
    this.appExports = appExports;
  }
}
