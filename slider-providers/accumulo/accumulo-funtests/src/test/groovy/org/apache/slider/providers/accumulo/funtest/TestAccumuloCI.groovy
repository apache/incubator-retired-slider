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
package org.apache.slider.providers.accumulo.funtest

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.test.continuous.ContinuousIngest
import org.apache.accumulo.test.continuous.ContinuousVerify
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.PortAssignments

/**
 * 
 */
@CompileStatic
@Slf4j
class TestAccumuloCI extends TestFunctionalAccumuloCluster {
  
  @Override
  String getClusterName() {
    return "test_accumulo_ci"
  }
  
  @Override
  public int getNumTservers() {
    return 2;
  }

  @Override
  public int getMonitorPort() {
    return PortAssignments._testAccumuloCI;
  }

  @Override
  void clusterLoadOperations(
      String clustername,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {
    assert clustername

    String currentUser = System.getProperty("user.name");
    String zookeepers = SLIDER_CONFIG.get(SliderXmlConfKeys.REGISTRY_ZK_QUORUM,
        FuntestProperties.DEFAULT_SLIDER_ZK_HOSTS)
    ZooKeeperInstance inst = new ZooKeeperInstance(currentUser + "-" + clustername, zookeepers)
    PasswordToken passwd = new PasswordToken(getPassword())
    Connector conn = inst.getConnector("root", new PasswordToken(getPassword()))
    
    // Create the test table with some split points
    String tableName = "testAccumuloCi";
    conn.tableOperations().create(tableName)
    TreeSet<Text> splits = new TreeSet<Text>()
    splits.add(new Text("2"))
    splits.add(new Text("5"))
    splits.add(new Text("7"))
    conn.tableOperations().addSplits(tableName, splits)
    
    // Write 15M records per tserver -- should take a few minutes
    String[] ciOpts = ["-i", inst.getInstanceName(),
      "-z", zookeepers, "-u", "root",
      "-p", getPassword(), "--table", tableName,
      "--num", Integer.toString(1000 * 1000 * 15 * getNumTservers()),
      "--batchMemory", "100000000",
      "--batchLatency", "600000",
      "--batchThreads", "1"]

    ContinuousIngest ci = new ContinuousIngest();
    ci.main(ciOpts);
    
    // Create a directory for the verify to write its output to
    Path verifyOutput = new Path("/user/" + currentUser + "/.slider/cluster/" + clustername + "/verify-output")
    assert !clusterFS.exists(verifyOutput)
    
    YarnConfiguration verifyConf = new YarnConfiguration(CommandTestBase.SLIDER_CONFIG);

        // Try to load the necessary classes for the Mappers to find them
    if (loadClassesForMapReduce(verifyConf)) {
      // If we found those classes, try to run in distributed mode.
      tryToLoadMapredSite(verifyConf)
    }
    
    // Run ContinuousVerify and ensure that no holes exist
    ContinuousVerify verify = new ContinuousVerify();
    String[] verifyOpts = ["-i", inst.getInstanceName(),
      "-z", zookeepers, "-u", "root",
      "-p", getPassword(), "--table", tableName,
      "--output", verifyOutput.toString(), "--maxMappers", Integer.toString(2 * getNumTservers()),
      "--reducers", Integer.toString(getNumTservers())]
    assert 0 == ToolRunner.run(verifyConf, verify, verifyOpts)
  }
}
