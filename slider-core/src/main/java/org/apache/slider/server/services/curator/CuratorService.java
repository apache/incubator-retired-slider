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

package org.apache.slider.server.services.curator;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class CuratorService extends AbstractService {
  protected static final Logger log =
    LoggerFactory.getLogger(CuratorService.class);
  protected final String basePath;

  private final CuratorFramework curator;
  private CuratorHelper curatorHelper;


  public CuratorService(String name,
                        CuratorFramework curator,
                        String basePath) {
    super(name);
    this.curator = Preconditions.checkNotNull(curator, "null client");
    this.basePath = basePath;
  }


  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    curatorHelper = new CuratorHelper(conf, basePath);
  }

  @Override
  protected void serviceStart() throws Exception {
    curator.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    closeCuratorComponent(curator);
  }

  public CuratorFramework getCurator() {
    return curator;
  }

  protected void closeCuratorComponent(Closeable closeable) {
    try {
      IOUtils.closeStream(closeable);
    } catch (Throwable ignored) {
      //triggered on an attempt to close before started
      log.debug("Error when closing {}", ignored);
    }
  }

  public String pathForName(String name) {
    return ZKPaths.makePath(getBasePath(), name);
  }

  protected String pathForInstance(String name, String id) {
    return ZKPaths.makePath(pathForName(name), id);
  }

  public String getBasePath() {
    return basePath;
  }

  public CuratorHelper getCuratorHelper() {
    return curatorHelper;
  }
}
