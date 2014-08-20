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
package org.apache.slider.providers.agent;

import org.apache.hadoop.fs.Path;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.providers.agent.application.metadata.DefaultConfig;
import org.apache.slider.providers.agent.application.metadata.DefaultConfigParser;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.MetainfoParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class AgentUtils {
  private static final Logger log = LoggerFactory.getLogger(AgentUtils.class);

  static Metainfo getApplicationMetainfo(SliderFileSystem fileSystem,
                                         String appDef) throws IOException {
    log.info("Reading metainfo at {}", appDef);
    InputStream metainfoStream = SliderUtils.getApplicationResourceInputStream(
        fileSystem.getFileSystem(), new Path(appDef), "metainfo.xml");
    if (metainfoStream == null) {
      log.error("metainfo.xml is unavailable at {}.", appDef);
      throw new IOException("metainfo.xml is required in app package.");
    }

    Metainfo metainfo = new MetainfoParser().parse(metainfoStream);

    return metainfo;
  }

  static DefaultConfig getDefaultConfig(SliderFileSystem fileSystem,
                                        String appDef, String configFileName)
      throws IOException {
    // this is the path inside the zip file
    String fileToRead = "configuration/" + configFileName;
    log.info("Reading default config file {} at {}", fileToRead, appDef);
    InputStream configStream = SliderUtils.getApplicationResourceInputStream(
        fileSystem.getFileSystem(), new Path(appDef), fileToRead);
    if (configStream == null) {
      log.error("{} is unavailable at {}.", fileToRead, appDef);
      throw new IOException("Expected config file " + fileToRead + " is not available.");
    }

    return new DefaultConfigParser().parse(configStream);
  }
}
