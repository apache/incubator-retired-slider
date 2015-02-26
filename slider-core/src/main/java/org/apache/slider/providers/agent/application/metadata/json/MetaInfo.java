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

import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the metadata associated with the application.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class MetaInfo {
  protected static final Logger
      log = LoggerFactory.getLogger(MetaInfo.class);

  private static final String UTF_8 = "UTF-8";

  /**
   * version
   */
  private String schemaVersion = "2.1";

  private Application application;


  /**
   * Creator.
   */
  public MetaInfo() {
  }

  public void setApplication(Application application) {
    this.application = application;
  }

  public Application getApplication() {
    return application;
  }

  public void setSchemaVersion(String version) {
    schemaVersion = version;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public static MetaInfo upConverted(Metainfo fromVersion2dot0) throws BadConfigException {
    if (fromVersion2dot0 == null) {
      return null;
    }

    MetaInfo metaInfo = new MetaInfo();

    if (fromVersion2dot0.getApplication() != null) {
      Application app = new Application();
      metaInfo.setApplication(app);

      app.setComment(fromVersion2dot0.getApplication().getComment());
      app.setExportedConfigs(fromVersion2dot0.getApplication().getExportedConfigs());
      app.setName(fromVersion2dot0.getApplication().getName());
      app.setVersion(fromVersion2dot0.getApplication().getVersion());

      // command order
      if (fromVersion2dot0.getApplication().getCommandOrder() != null) {
        for (org.apache.slider.providers.agent.application.metadata.CommandOrder xmlCo :
            fromVersion2dot0.getApplication().getCommandOrder()) {
          CommandOrder co = new CommandOrder();
          co.setCommand(xmlCo.getCommand());
          co.setRequires(xmlCo.getRequires());
          app.getCommandOrders().add(co);
        }
      }

      // packages
      if (fromVersion2dot0.getApplication().getOSSpecifics() != null) {
        for (org.apache.slider.providers.agent.application.metadata.OSSpecific xmlOSS :
            fromVersion2dot0.getApplication().getOSSpecifics()) {
          for (org.apache.slider.providers.agent.application.metadata.OSPackage xmlOSP : xmlOSS.getPackages()) {
            Package pkg = new Package();
            pkg.setName(xmlOSP.getName());
            pkg.setType(xmlOSP.getType());
            app.getPackages().add(pkg);
          }
        }
      }

      // export
      if (fromVersion2dot0.getApplication().getExportGroups() != null) {
        for (org.apache.slider.providers.agent.application.metadata.ExportGroup xmlEg :
            fromVersion2dot0.getApplication().getExportGroups()) {
          ExportGroup eg = new ExportGroup();
          eg.setName(xmlEg.getName());
          app.getExportGroups().add(eg);
          for (org.apache.slider.providers.agent.application.metadata.Export xmlEx :
              xmlEg.getExports()) {
            Export exp = new Export();
            exp.setName(xmlEx.getName());
            exp.setValue(xmlEx.getValue());
            eg.getExports().add(exp);
          }
        }
      }

      // config file
      if (fromVersion2dot0.getApplication().getConfigFiles() != null) {
        for (org.apache.slider.providers.agent.application.metadata.ConfigFile xmlConf :
            fromVersion2dot0.getApplication().getConfigFiles()) {
          ConfigFile conf = new ConfigFile();
          conf.setDictionaryName(xmlConf.getDictionaryName());
          conf.setFileName(xmlConf.getFileName());
          conf.setType(xmlConf.getType());
          app.getConfigFiles().add(conf);
        }
      }

      // component
      if (fromVersion2dot0.getApplication().getComponents() != null) {
        for (org.apache.slider.providers.agent.application.metadata.Component xmlComp :
            fromVersion2dot0.getApplication().getComponents()) {
          Component comp = new Component();
          comp.setAppExports(xmlComp.getAppExports());
          comp.setAutoStartOnFailure(xmlComp.getRequiresAutoRestart());
          comp.setCategory(xmlComp.getCategory());
          comp.setCompExports(xmlComp.getCompExports());
          comp.setMaxInstanceCount(xmlComp.getMaxInstanceCountInt());
          comp.setMinInstanceCount(xmlComp.getMinInstanceCountInt());
          comp.setName(xmlComp.getName());
          comp.setPublishConfig(xmlComp.getPublishConfig());
          if(xmlComp.getCommandScript() != null) {
            CommandScript commandScript = new CommandScript();
            commandScript.setScript(xmlComp.getCommandScript().getScript());
            commandScript.setScriptType(xmlComp.getCommandScript().getScriptType());
            commandScript.setTimeout(xmlComp.getCommandScript().getTimeout());
            comp.setCommandScript(commandScript);
          }
          app.getComponents().add(comp);
        }
      }

    }
    return metaInfo;
  }

  public Component getApplicationComponent(String roleName) {
    if (application == null) {
      log.error("Malformed app definition: Expect application as the top level element for metainfo");
    } else {
      for (Component component : application.getComponents()) {
        if (component.getName().equals(roleName)) {
          return component;
        }
      }
    }
    return null;
  }
}