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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.registry.client.binding.zk.ZKPathDumper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class CuratorService extends AbstractService {
  private static final Logger log =
    LoggerFactory.getLogger(CuratorService.class);
  protected final String basePath;

  private final CuratorFramework curator;
  private CuratorHelper curatorHelper;


  public CuratorService(String name,
                        CuratorFramework curator,
                        String basePath) {
    super(name);
    this.curator = Preconditions.checkNotNull(curator, "null curator");
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

  public final CuratorFramework getCurator() {
    return curator;
  }

  protected final void closeCuratorComponent(Closeable closeable) {
    try {
      IOUtils.closeStream(closeable);
    } catch (Throwable ignored) {
      //triggered on an attempt to close before started
      log.debug("Error when closing {}", ignored);
    }
  }

  public final String pathForServicetype(String servicetype) {
    return ZKPaths.makePath(getBasePath(), servicetype);
  }

  protected final String pathForInstance(String servicetype, String id) {
    Preconditions.checkNotNull(servicetype);
    Preconditions.checkNotNull(id);
    return ZKPaths.makePath(pathForServicetype(servicetype), id);
  }

  public final String getBasePath() {
    return basePath;
  }

  public final CuratorHelper getCuratorHelper() {
    return curatorHelper;
  }

  @Override
  public String toString() {
    return super.toString() + "; "
           + (curatorHelper != null ? curatorHelper : "( unbound)")
           + "; " + basePath;
  }

  /**
   * Get an on-demand path jumper
   * @return a class that can dump the contents of the registry
   */
  public ZKPathDumper dumpPath() {
    return new ZKPathDumper(curator, basePath);
  }
}
