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

package org.apache.slider.test

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.registry.client.api.RegistryOperations
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService
import org.apache.hadoop.registry.server.services.MicroZookeeperService
import org.apache.slider.common.tools.SliderUtils

@Slf4j
@CompileStatic
class MicroZKCluster implements Closeable {

  public static final String HOSTS = "127.0.0.1"
  MicroZookeeperService zkService
  String zkBindingString
  final Configuration conf
  public RegistryOperations registryOperations

  MicroZKCluster() {
    this(SliderUtils.createConfiguration())
  }

  MicroZKCluster(Configuration conf) {
    this.conf = conf
  }

  void createCluster(String name) {
    zkService = new MicroZookeeperService(name)
    
    zkService.init(conf)
    zkService.start()
    zkBindingString = zkService.connectionString
    log.info("Created $this")
    registryOperations = new RegistryOperationsService(
        "registry",
        zkService)
    registryOperations.init(conf)
    registryOperations.start()
  }

  @Override
  void close() throws IOException {
    registryOperations?.stop()
    zkService?.stop()
  }

  @Override
  String toString() {
    return "Micro ZK cluster as $zkBindingString"
  }
  

}
