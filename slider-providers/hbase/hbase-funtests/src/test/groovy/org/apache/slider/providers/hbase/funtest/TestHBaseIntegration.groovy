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

package org.apache.slider.providers.hbase.funtest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.IntegrationTestIngest
import org.apache.hadoop.hbase.IntegrationTestingUtility
import org.apache.hadoop.util.ToolRunner
import org.apache.slider.api.ClusterDescription
import org.apache.slider.providers.hbase.HBaseConfigFileOptions;

/* Runs IntegrationTestIngest on cluster
 *
 * Note: this test runs for about 20 minutes
 * please set slider.test.timeout.seconds accordingly
 */
class TestHBaseIntegration extends TestFunctionalHBaseCluster {

  @Override
  String getClusterName() {
    return "test_hbase_integration"
  }

  @Override
  void clusterLoadOperations(
      String clustername,
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {
    String parent = "/yarnapps_slider_yarn_" + clustername
    clientConf.set(HBaseConfigFileOptions.KEY_ZNODE_PARENT, parent)

    clientConf.set(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER, "true")

    String[] args = []
    IntegrationTestIngest test = new IntegrationTestIngest();
    test.setConf(clientConf)
    int ret = ToolRunner.run(clientConf, test, args);
    assert ret == 0;
  }


  public int getWorkerPortAssignment() {
    return 0
  }

  public int getMasterPortAssignment() {
    return 0
  }
}
