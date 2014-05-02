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

import org.apache.commons.digester.Digester;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class MetainfoParser {

  public Metainfo parse(InputStream metainfoStream) throws IOException {
    Digester digester = new Digester();
    digester.setValidating(false);

    digester.addObjectCreate("metainfo", Metainfo.class);
    digester.addBeanPropertySetter("metainfo/schemaVersion");

    digester.addObjectCreate("*/service", Service.class);
    digester.addBeanPropertySetter("*/service/name");
    digester.addBeanPropertySetter("*/service/comment");
    digester.addBeanPropertySetter("*/service/version");

    digester.addObjectCreate("*/component", Component.class);
    digester.addBeanPropertySetter("*/component/name");
    digester.addBeanPropertySetter("*/component/category");
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

    digester.addObjectCreate("*/configuration-dependencies",
                             ConfigurationDependencies.class);
    digester.addBeanPropertySetter("*/config-type", "configType");
    digester.addSetNext("*/configuration-dependencies", "setConfigDependencies");

    digester.addSetNext("*/service", "addService");

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
