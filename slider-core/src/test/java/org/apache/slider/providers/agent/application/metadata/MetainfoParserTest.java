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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class MetainfoParserTest {
  protected static final Logger log =
      LoggerFactory.getLogger(MetainfoParserTest.class);
  public static final String METAINFO_XML =
      "/org/apache/slider/providers/agent/application/metadata/metainfo.xml";

  @Test
  public void testParse() throws IOException {

    InputStream resStream = this.getClass().getResourceAsStream(
        METAINFO_XML);
    MetainfoParser parser = new MetainfoParser();
    Metainfo metainfo = parser.parse(resStream);
    assert metainfo != null;
    assert metainfo.services.size() == 1;
    Service service = metainfo.getServices().get(0);
    assert "STORM".equals(service.getName());
    assert 5 == service.getComponents().size();
    OSPackage pkg = service.getOSSpecifics().get(0).getPackages().get(0);
    assert "tarball".equals(pkg.getType());
    assert "files/apache-storm-0.9.1.2.1.1.0-237.tar.gz".equals(pkg.getName());
    boolean found = false;
    for (Component comp : service.getComponents()) {
      if (comp != null && comp.getName().equals("NIMBUS")) {
        found = true;
      }
    }
    assert found;
  }
}
