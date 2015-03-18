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
package org.apache.slider.providers.agent.application.metadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.digester.Digester;
import org.apache.commons.io.IOUtils;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 *
 */
public class MetainfoParser {
  private final GsonBuilder gsonBuilder = new GsonBuilder();
  private final Gson gson;

  public MetainfoParser() {
    gson = gsonBuilder.create();
  }

  /**
   * Convert to a JSON string
   *
   * @return a JSON string description
   *
   * @throws IOException Problems mapping/writing the object
   */
  public String toJsonString(Metainfo metaInfo) throws IOException {
    return gson.toJson(metaInfo);
  }

  /**
   * Convert from JSON
   *
   * @param json input
   *
   * @return the parsed JSON
   *
   * @throws IOException IO
   */
  public Metainfo fromJsonString(String json)
      throws IOException {
    return gson.fromJson(json, Metainfo.class);
  }

  /**
   * Parse metainfo from an IOStream
   *
   * @param is
   *
   * @return
   *
   * @throws IOException
   */
  public Metainfo fromJsonStream(InputStream is) throws IOException {
    StringWriter writer = new StringWriter();
    IOUtils.copy(is, writer);
    return fromJsonString(writer.toString());
  }


  /**
   * Parse metainfo from an XML formatted IOStream
   *
   * @param metainfoStream
   *
   * @return
   *
   * @throws IOException
   */
  public Metainfo fromXmlStream(InputStream metainfoStream) throws IOException {
    Digester digester = new Digester();
    digester.setValidating(false);

    digester.addObjectCreate("metainfo", Metainfo.class);
    digester.addBeanPropertySetter("metainfo/schemaVersion");

    digester.addObjectCreate("*/application", Application.class);
    digester.addBeanPropertySetter("*/application/name");
    digester.addBeanPropertySetter("*/application/comment");
    digester.addBeanPropertySetter("*/application/version");
    digester.addBeanPropertySetter("*/application/exportedConfigs");

    digester.addObjectCreate("*/commandOrder", CommandOrder.class);
    digester.addBeanPropertySetter("*/commandOrder/command");
    digester.addBeanPropertySetter("*/commandOrder/requires");
    digester.addSetNext("*/commandOrder", "addCommandOrder");

    digester.addObjectCreate("*/exportGroup", ExportGroup.class);
    digester.addBeanPropertySetter("*/exportGroup/name");
    digester.addObjectCreate("*/export", Export.class);
    digester.addBeanPropertySetter("*/export/name");
    digester.addBeanPropertySetter("*/export/value");
    digester.addSetNext("*/export", "addExport");
    digester.addSetNext("*/exportGroup", "addExportGroup");

    digester.addObjectCreate("*/component", Component.class);
    digester.addBeanPropertySetter("*/component/name");
    digester.addBeanPropertySetter("*/component/category");
    digester.addBeanPropertySetter("*/component/publishConfig");
    digester.addBeanPropertySetter("*/component/minInstanceCount");
    digester.addBeanPropertySetter("*/component/maxInstanceCount");
    digester.addBeanPropertySetter("*/component/autoStartOnFailure");
    digester.addBeanPropertySetter("*/component/appExports");
    digester.addBeanPropertySetter("*/component/compExports");
    digester.addObjectCreate("*/componentExport", ComponentExport.class);
    digester.addBeanPropertySetter("*/componentExport/name");
    digester.addBeanPropertySetter("*/componentExport/value");
    digester.addSetNext("*/componentExport", "addComponentExport");
    digester.addSetNext("*/component", "addComponent");

    digester.addObjectCreate("*/commandScript", CommandScript.class);
    digester.addBeanPropertySetter("*/commandScript/script");
    digester.addBeanPropertySetter("*/commandScript/scriptType");
    digester.addBeanPropertySetter("*/commandScript/timeout");
    digester.addSetNext("*/commandScript", "addCommandScript");

    digester.addObjectCreate("*/osSpecific", OSSpecific.class);
    digester.addBeanPropertySetter("*/osSpecific/osType");
    digester.addObjectCreate("*/package", OSPackage.class);
    digester.addBeanPropertySetter("*/package/type");
    digester.addBeanPropertySetter("*/package/name");
    digester.addSetNext("*/package", "addOSPackage");
    digester.addSetNext("*/osSpecific", "addOSSpecific");

    digester.addObjectCreate("*/configFile", ConfigFile.class);
    digester.addBeanPropertySetter("*/configFile/type");
    digester.addBeanPropertySetter("*/configFile/fileName");
    digester.addBeanPropertySetter("*/configFile/dictionaryName");
    digester.addSetNext("*/configFile", "addConfigFile");

    digester.addSetRoot("*/application", "setApplication");

    try {
      return (Metainfo) digester.parse(metainfoStream);
    } catch (IOException e) {

    } catch (SAXException e) {

    } finally {
      metainfoStream.close();
    }

    return null;
  }
}
