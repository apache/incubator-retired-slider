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

package org.apache.slider.core.registry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.client.SliderYarnClientImpl;

import java.io.IOException;
import java.util.List;

/**
 * Client code for interacting with a registry of service instances.
 * The initial logic just enumerates service instances in the YARN RM
 */
public class YARNRegistryClient {

  final SliderYarnClientImpl yarnClient;
  final String username;
  final Configuration conf;


  public YARNRegistryClient(SliderYarnClientImpl yarnClient,
                            String username,
                            Configuration conf) {
    this.yarnClient = yarnClient;
    this.username = username;
    this.conf = conf;
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param appname application name
   * @return the list of all matching application instances
   */
  public List<ApplicationReport> findAllLiveInstances(String appname)
    throws YarnException, IOException {

    return yarnClient.findAllLiveInstances(username, appname);
  }


  /**
   * Find an instance of a application belong to the current user
   * @param appname application name
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  public ApplicationReport findInstance(String appname) throws
                                                        YarnException,
                                                        IOException {
    List<ApplicationReport> instances = listInstances();
    return yarnClient.findClusterInInstanceList(instances, appname);
  }

  /**
   * List instances belonging to a specific user
   * @return a possibly empty list of AMs
   */
  public List<ApplicationReport> listInstances()
    throws YarnException, IOException {
    return yarnClient.listInstances(username);
  }


}
