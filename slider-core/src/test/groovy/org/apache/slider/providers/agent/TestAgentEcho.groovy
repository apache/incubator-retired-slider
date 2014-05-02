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

package org.apache.slider.providers.agent

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.api.ResourceKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

import static org.apache.slider.common.params.Arguments.*
import static org.apache.slider.providers.agent.AgentKeys.*

/**
 * Tests an echo command
 */
@CompileStatic
@Slf4j
class TestAgentEcho extends AgentTestBase {


  @Override
  void checkTestAssumptions(YarnConfiguration conf) {
    
  }

  @Test
  public void testEchoOperation() throws Throwable {
    def clustername = "test_agent_echo"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)

    File slider_core = new File(new File(".").absoluteFile, "src/test/python");
    String echo_py = "echo.py"
    File echo_py_path = new File(slider_core, echo_py)
    String app_def = "appdef_1.tar"
    File app_def_path = new File(slider_core, app_def)
    String agt_ver = "version"
    File agt_ver_path = new File(slider_core, agt_ver)
    String agt_conf = "agent.ini"
    File agt_conf_path = new File(slider_core, agt_conf)
    assert echo_py_path.exists()
    assert app_def_path.exists()
    assert agt_ver_path.exists()
    assert agt_conf_path.exists()

    def role = "echo"
    Map<String, Integer> roles = [
        (role): 1,
    ];
    ServiceLauncher<SliderClient> launcher = buildAgentCluster(clustername,
        roles,
        [
            ARG_OPTION, PACKAGE_PATH, slider_core.absolutePath,
            ARG_OPTION, APP_DEF, "file://" + app_def_path.absolutePath,
            ARG_OPTION, AGENT_CONF, "file://" + agt_conf_path.absolutePath,
            ARG_OPTION, AGENT_VERSION, "file://" + agt_ver_path.absolutePath,
            ARG_RES_COMP_OPT, role, ResourceKeys.COMPONENT_PRIORITY, "1",
            ARG_COMP_OPT, role, SCRIPT_PATH, echo_py,
            ARG_COMP_OPT, role, SERVICE_NAME, "Agent",
        ],
        true, true,
        true)
    SliderClient sliderClient = launcher.service

    waitForRoleCount(sliderClient, roles, AGENT_CLUSTER_STARTUP_TIME)
    //sleep a bit
    sleep(20000)
    //expect the role count to be the same
    waitForRoleCount(sliderClient, roles, 1000)

  }
}
