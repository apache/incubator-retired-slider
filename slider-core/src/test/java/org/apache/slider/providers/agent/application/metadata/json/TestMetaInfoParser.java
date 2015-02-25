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
package org.apache.slider.providers.agent.application.metadata.json;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TestMetaInfoParser {
  protected static final Logger log =
      LoggerFactory.getLogger(TestMetaInfoParser.class);

  @Test
  public void testParse() throws IOException {
    String metaInfo1_json = "{\n"
                            + "\"schemaVersion\":\"2.2\",\n"
                            + "\"application\":{\n"
                            +     "\"name\": \"MEMCACHED\","
                            +     "\"exportGroups\": ["
                            +        "{"
                            +          "\"name\": \"Servers\","
                            +          "\"exports\": ["
                            +            "{"
                            +               "\"name\": \"host_port\","
                            +               "\"value\": \"${MEMCACHED_HOST}:${site.global.port}\""
                            +            "}"
                            +          "]"
                            +        "}"
                            +      "],"
                            +     "\"components\": ["
                            +        "{"
                            +          "\"name\": \"MEMCACHED\","
                            +          "\"compExports\": \"Servers-host_port\","
                            +          "\"commands\": ["
                            +            "{"
                            +               "\"exec\": \"java -classpath /usr/myapps/memcached/*:/usr/lib/hadoop/lib/* com.thimbleware.jmemcached.Main\""
                            +            "}"
                            +          "]"
                            +        "},"
                            +        "{"
                            +          "\"name\": \"MEMCACHED2\","
                            +          "\"commands\": ["
                            +            "{"
                            +               "\"exec\": \"scripts/config.py\","
                            +               "\"type\": \"PYTHON\","
                            +               "\"name\": \"CONFIGURE\""
                            +            "}"
                            +          "],"
                            +          "\"containers\": ["
                            +            "{"
                            +               "\"name\": \"redis\","
                            +               "\"image\": \"dockerhub/redis\","
                            +               "\"options\": \"-net=bridge\","
                            +               "\"mounts\": ["
                            +                 "{"
                            +                   "\"containerMount\": \"/tmp/conf\","
                            +                   "\"hostMount\": \"{$conf:@//site/global/app_root}/conf\""
                            +                 "}"
                            +               "]"
                            +            "}"
                            +          "]"
                            +        "}"
                            +      "]"
                            +   "}"
                            + "}";

    MetaInfoParser parser = new MetaInfoParser();
    MetaInfo mInfo = parser.fromJsonString(metaInfo1_json);
    Assert.assertEquals("2.2", mInfo.getSchemaVersion());

    Application app = mInfo.getApplication();
    Assert.assertNotNull(app);

    Assert.assertEquals("MEMCACHED", app.getName());
    List<ExportGroup> egs = app.getExportGroups();
    Assert.assertEquals(1, egs.size());
    ExportGroup eg = egs.get(0);
    Assert.assertEquals("Servers", eg.getName());
    List<Export> exports = eg.getExports();
    Assert.assertEquals(1, exports.size());
    Export export = exports.get(0);
    Assert.assertEquals("host_port", export.getName());
    Assert.assertEquals("${MEMCACHED_HOST}:${site.global.port}", export.getValue());

    List<Component> components = app.getComponents();
    Assert.assertEquals(2, components.size());

    Component c1 = app.getApplicationComponent("MEMCACHED");
    Assert.assertNotNull(c1);
    Assert.assertEquals("MEMCACHED", c1.getName());
    Assert.assertEquals("Servers-host_port", c1.getCompExports());
    Assert.assertEquals(1, c1.getCommands().size());
    ComponentCommand cmd = c1.getCommands().get(0);
    Assert.assertEquals("START", cmd.getName());
    Assert.assertEquals("SHELL", cmd.getType());
    Assert.assertEquals("java -classpath /usr/myapps/memcached/*:/usr/lib/hadoop/lib/* com.thimbleware.jmemcached.Main", cmd.getExec());

    Component c2 = app.getApplicationComponent("MEMCACHED2");
    Assert.assertNotNull(c2);
    Assert.assertEquals("MEMCACHED2", c2.getName());
    Assert.assertEquals(1, c2.getCommands().size());
    cmd = c2.getCommands().get(0);
    Assert.assertEquals("CONFIGURE", cmd.getName());
    Assert.assertEquals("PYTHON", cmd.getType());
    Assert.assertEquals("scripts/config.py", cmd.getExec());
    Assert.assertEquals(1, c2.getContainers().size());
    Container cont = c2.getContainers().get(0);
    Assert.assertEquals("redis", cont.getName());
    Assert.assertEquals(1, cont.getMounts().size());
    ContainerMount mount = cont.getMounts().get(0);
    Assert.assertEquals("{$conf:@//site/global/app_root}/conf", mount.getHostMount());
  }
}
