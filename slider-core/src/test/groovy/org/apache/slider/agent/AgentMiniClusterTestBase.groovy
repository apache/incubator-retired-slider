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

package org.apache.slider.agent

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderXMLConfKeysForTesting
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.agent.AgentKeys
import org.apache.slider.test.YarnZKMiniClusterTestBase
import org.junit.BeforeClass

/**
 * test base for agent clusters
 */
@CompileStatic
@Slf4j
public abstract class AgentMiniClusterTestBase
    extends YarnZKMiniClusterTestBase {
  private static File agentConf
  private static File agentDef
  private static File imagePath


  @BeforeClass
  public static void createSubConfFiles() {
    File destDir = new File("target/agent_minicluster_testbase")
    destDir.mkdirs()
    agentConf = new File(destDir, "agentconf")
    agentConf.createNewFile()
    agentDef = new File(destDir, "agentdef")
    agentDef.createNewFile()
    imagePath = new File(destDir, "imagePath.tar.gz")
    imagePath.createNewFile()
    
  }
  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + AgentKeys.CONF_RESOURCE;
  }

  @Override
  void setup() {
    super.setup()
    def testConf = getTestConfiguration()
  }

  /**
   * Teardown kills region servers
   */
  @Override
  void teardown() {
    super.teardown();
    if (teardownKillall) {

    }
  }

  @Override
  String getApplicationHomeKey() {
    return SliderXMLConfKeysForTesting.KEY_TEST_AGENT_HOME;
  }

  @Override
  String getArchiveKey() {
    return SliderXMLConfKeysForTesting.KEY_TEST_AGENT_TAR;
  }

  /**
   return a mock home dir -this test case does not intend to run any agent
   */
  @Override
  List<String> getImageCommands() {
    [Arguments.ARG_IMAGE, imagePath.toURI().toString()]
  }

/**
 * Create an AM without a master
 * @param clustername AM name
 * @param size # of nodes
 * @param deleteExistingData should any existing cluster data be deleted
 * @param blockUntilRunning block until the AM is running
 * @return launcher which will have executed the command.
 */
  public ServiceLauncher<SliderClient> createMasterlessAM(
      String clustername,
      int size,
      boolean deleteExistingData,
      boolean blockUntilRunning) {
    return createCluster(clustername,
        [:],
        [

        ],
        deleteExistingData,
        blockUntilRunning,
        [
            (AgentKeys.APP_DEF): agentConf.toURI().toString(),
            (AgentKeys.AGENT_CONF): agentDef.toURI().toString()
        ])
  }

}
