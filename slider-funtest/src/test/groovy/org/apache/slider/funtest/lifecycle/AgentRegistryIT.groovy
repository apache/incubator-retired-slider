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
import org.apache.hadoop.registry.client.binding.RegistryUtils
import org.apache.hadoop.registry.client.types.Endpoint
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
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
public class AgentRegistryIT extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-agent-registry"

  static String APP_RESOURCE2 = "../slider-core/src/test/app_packages/test_command_log/resources_no_role.json"


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
    describe("Create a 0-role cluster and make registry queries against it")
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    //at this point the cluster should exist.
    assertPathExists(
        clusterFS,
        "Cluster parent directory does not exist",
        clusterpath.parent)

    assertPathExists(clusterFS, "Cluster directory does not exist", clusterpath)

    // resolve the ~ path

    resolve(0, [ARG_LIST, ARG_PATH, "/"]).dumpOutput()
    resolve(0, [ARG_LIST, ARG_PATH, "/users"]).dumpOutput()


    def home = homepath()
    resolve(0, [ARG_LIST, ARG_PATH, home]).dumpOutput()


    String sliderApps = "${home}/services/${SliderKeys.APP_TYPE}"
    resolve(0, [ARG_LIST, ARG_PATH, sliderApps]).dumpOutput()

    // running app
    String appPath = sliderApps +"/"+ CLUSTER
    resolve(0, [ARG_LIST, ARG_PATH, appPath]).dumpOutput()

    resolve(0, [ARG_PATH, appPath]).dumpOutput()
    // and the service record
    File serviceRecordFile = File.createTempFile("tempfile", ".json")
    serviceRecordFile.deleteOnExit()
    resolve(0, [ARG_PATH, appPath,
                ARG_OUTPUT, serviceRecordFile.absolutePath])
    RegistryUtils.ServiceRecordMarshal marshal = new RegistryUtils.ServiceRecordMarshal()

    ServiceRecord serviceRecord = marshal.fromFile(serviceRecordFile)
/*
    def ipcEndpoint = serviceRecord.external.find { Endpoint epr ->
      epr.api == AM_IPC_PROTOCOL;
    }
    assert ipcEndpoint != null;
*/
    def endpoints = [:]
    serviceRecord.external.each { Endpoint epr ->
      endpoints[epr.api] = epr;
    }
    serviceRecord.internal.each { Endpoint epr ->
      endpoints[epr.api] = epr;
    }
    assert endpoints[PUBLISHER_REST_API]
    assert endpoints[REGISTRY_REST_API]
    assert endpoints[AGENT_SECURE_REST_API]
    assert endpoints[AGENT_ONEWAY_REST_API]

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


    repeatUntilSuccess("probe for missing registry entry",
        this.&probeForEntryMissing, 10000, 1000,
        [path: appPath],
        true,
        "registry entry never deleted") {
      // await the registry lookup to fail
      resolve(EXIT_NOT_FOUND, [ARG_PATH, appPath])
    }
  }

  /**
   * Return the home registry path
   * @return
   */
  public String homepath() {
    return RegistryUtils.homePathForCurrentUser()
  }


  Outcome probeForEntryMissing(Map args) {
    String path = args["path"]
    def shell = slider([ACTION_RESOLVE, ARG_PATH, path])
    return Outcome.fromBool(shell.ret == EXIT_NOT_FOUND)
  }
}
