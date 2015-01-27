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

package org.apache.slider.server.appmaster

import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ResourceKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionKillContainerArgs
import org.apache.slider.core.build.InstanceBuilder
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.launch.LaunchedApplication
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.LockAcquireFailedException
import org.apache.slider.providers.agent.AgentKeys
import org.apache.slider.providers.agent.AgentTestBase
import org.junit.Before
import org.junit.Test

import static org.apache.slider.common.params.Arguments.*
import static org.apache.slider.providers.agent.AgentKeys.*

/**
 * Tests an echo command
 */
@Slf4j
class TestDelayInContainerLaunch extends AgentTestBase {

  File slider_core
  String echo_py
  File echo_py_path
  File app_def_path
  String agt_ver
  File agt_ver_path
  String agt_conf
  File agt_conf_path
  
  @Before
  public void setupArtifacts() {
    slider_core = new File(new File(".").absoluteFile, "src/test/python");
    echo_py = "echo.py"
    echo_py_path = new File(slider_core, echo_py)
    app_def_path = new File(app_def_pkg_path)
    agt_ver = "version"
    agt_ver_path = new File(slider_core, agt_ver)
    agt_conf = "agent.ini"
    agt_conf_path = new File(slider_core, agt_conf)

  }
  
  @Override
  void checkTestAssumptions(YarnConfiguration conf) {

  }

  @Test
  public void testDelayInContainerLaunch() throws Throwable {
    String clustername = createMiniCluster("",
        configuration,
        1,
        1,
        1,
        true,
        false)

    assert echo_py_path.exists()
    assert app_def_path.exists()
    assert agt_ver_path.exists()
    assert agt_conf_path.exists()

    def role = "echo"
    Map<String, Integer> roles = [
        (role): 1,
    ];
    long delay = 30

    DelayingSliderClient.delay = delay
    sliderClientClassName = DelayingSliderClient.name
    try {
      ServiceLauncher<SliderClient> launcher = buildAgentCluster(clustername,
        roles,
        [
            ARG_OPTION, PACKAGE_PATH, slider_core.absolutePath,
            ARG_OPTION, APP_DEF, toURIArg(app_def_path),
            ARG_OPTION, AGENT_CONF, toURIArg(agt_conf_path),
            ARG_OPTION, AGENT_VERSION, toURIArg(agt_ver_path),
            ARG_RES_COMP_OPT, role, ResourceKeys.COMPONENT_PRIORITY, "1",
            ARG_COMP_OPT, role, SCRIPT_PATH, echo_py,
            ARG_COMP_OPT, role, SERVICE_NAME, "Agent",
            ARG_COMP_OPT, role, TEST_RELAX_VERIFICATION, "true",
        ],
        true, true,
        true)
      SliderClient sliderClient = launcher.service
      waitForRoleCount(sliderClient, roles, AGENT_CLUSTER_STARTUP_TIME)

      ClusterDescription status = sliderClient.clusterDescription
      def workers = status.instances["echo"]
      assert workers.size() == 1
      def worker1 = workers[0]

      // set the delay for 10 seconds more than start duration
      ActionKillContainerArgs args = new ActionKillContainerArgs();
      args.id = worker1
      long start = System.currentTimeMillis()
      assert 0 == sliderClient.actionKillContainer(clustername, args)
      sleep(5000)
      waitForRoleCount(sliderClient, roles, AGENT_CLUSTER_STARTUP_TIME)
      long duration = System.currentTimeMillis() - start
      assert duration/1000 >= delay

    } finally {
      sliderClientClassName = DEFAULT_SLIDER_CLIENT
    }


  }

  static class DelayingSliderClient extends SliderClient {
    
    
    static long delay
    @Override
    protected void persistInstanceDefinition(boolean overwrite,
                                             Path appconfdir,
                                             InstanceBuilder builder)
    throws IOException, SliderException, LockAcquireFailedException {
      AggregateConf conf = builder.getInstanceDescription()
      conf.getAppConfOperations().getGlobalOptions().put(
          AgentKeys.KEY_CONTAINER_LAUNCH_DELAY,
          String.valueOf(delay))
      super.persistInstanceDefinition(overwrite, appconfdir, builder)
    }

    @Override
    LaunchedApplication launchApplication(String clustername,
                                          Path clusterDirectory,
                                          AggregateConf instanceDefinition,
                                          boolean debugAM)
    throws YarnException, IOException {
      instanceDefinition.getAppConfOperations().getGlobalOptions().put(
          AgentKeys.KEY_CONTAINER_LAUNCH_DELAY,
          String.valueOf(delay))
      return super.launchApplication(clustername, clusterDirectory, instanceDefinition, debugAM)
    }

    public static void setDelay (long aDelay) {
      delay = aDelay
    }
  }
}
