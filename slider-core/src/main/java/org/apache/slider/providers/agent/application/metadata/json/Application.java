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

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the metadata associated with the application.
 */
public class Application {
  protected static final Logger
      log = LoggerFactory.getLogger(Application.class);


  private String name;
  private List<Component> components = new ArrayList<>();
  private String comment;
  private String version;
  private String exportedConfigs;
  private List<ExportGroup> exportGroups = new ArrayList<>();
  private List<Package> packages = new ArrayList<>();
  private List<CommandOrder> commandOrders = new ArrayList<>();
  private List<ConfigFile> configFiles = new ArrayList<>();

  public Application() {
  }

  @JsonProperty("components")
  public List<Component> getComponents() {
    return this.components;
  }

  @JsonProperty("exportGroups")
  public List<ExportGroup> getExportGroups() {
    return this.exportGroups;
  }

  @JsonProperty("packages")
  public List<Package> getPackages() {
    return this.packages;
  }

  @JsonProperty("commandOrders")
  public List<CommandOrder> getCommandOrders() {
    return this.commandOrders;
  }

  @JsonProperty("configFiles")
  public List<ConfigFile> getConfigFiles() {
    return this.configFiles;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @JsonIgnore
  public Component getApplicationComponent(String name) {
    for (Component component : components) {
      if (name.equals(component.getName())) {
        return component;
      }
    }
    return null;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getExportedConfigs() {
    return exportedConfigs;
  }

  public void setExportedConfigs(String exportedConfigs) {
    this.exportedConfigs = exportedConfigs;
  }
}