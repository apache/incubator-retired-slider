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

package org.apache.slider.funtest.lifecycle

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovy.json.*
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.registry.client.binding.RegistryUtils
import org.apache.hadoop.registry.client.types.Endpoint
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.test.Outcome

import static org.apache.slider.core.registry.info.CustomRegistryConstants.*
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AgentPingSocketIT extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-agent-ping-port"

  static String APP_RESOURCE12 = "../slider-core/src/test/app_packages/test_min_pkg/nc_ping_cmd/resources.json"
  static String APP_META12 = "../slider-core/src/test/app_packages/test_min_pkg/nc_ping_cmd/metainfo.json"
  static String APP_TEMPLATE12 = "../slider-core/src/test/app_packages/test_min_pkg/nc_ping_cmd/appConfig.json"


  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testAgentRegistry() throws Throwable {
    describe("Create a cluster using metainfo, resources, and appConfig that calls nc to listen on a port")
    assumeNotWindows()
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createSliderApplicationMinPkg(CLUSTER,
        APP_META12,
        APP_RESOURCE12,
        APP_TEMPLATE12,
        [],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    describe("Checking the exported port value and pinging it for " + CLUSTER)
    def outfile = tmpFile(".txt")

    def commands = [
        ACTION_REGISTRY,
        ARG_NAME,
        CLUSTER,
        ARG_LISTEXP,
        ARG_OUTPUT,
        outfile.absolutePath
    ]

    awaitRegistryOutfileContains(outfile, commands, "servers")

    // get Servers host_port folders
    slider(EXIT_SUCCESS,
        [
            ACTION_REGISTRY,
            ARG_NAME,
            CLUSTER,
            ARG_GETEXP,
            "servers",
            ARG_OUTPUT,
            outfile.absolutePath])

    describe(outfile.absolutePath)

    def result = new JsonSlurper().parseText(outfile.text)
    Map jsonResult = (Map) result
    List host_ports = (List)jsonResult.get("host_port")
    Map host_port = (Map)host_ports[0]
    String host_port_val = host_port.get("value")
    def tokens = host_port_val.tokenize(':')
    def host = tokens[0]
    def port = tokens[1].toInteger()

    try {
      def socket = new Socket();
      def addr = new InetSocketAddress(host, port)
      socket.connect(addr, 2000)
      socket.close()
    } catch (IOException e) {
      throw NetUtils.wrapException(host, port, "localhost", 0, e)
    }

    //stop
    freeze(0, CLUSTER,
        [
            ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
            ARG_MESSAGE, "final-shutdown"
        ])

    assertInYarnState(appId, YarnApplicationState.FINISHED)
    destroy(0, CLUSTER)

    //cluster now missing
    exists(EXIT_UNKNOWN_INSTANCE, CLUSTER)
  }
}
