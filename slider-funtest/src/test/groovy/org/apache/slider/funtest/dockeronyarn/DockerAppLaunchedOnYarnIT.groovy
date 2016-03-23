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
package org.apache.slider.funtest.dockeronyarn

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ClusterNode
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.api.StateValues
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.funtest.framework.FileUploader
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class DockerAppLaunchedOnYarnIT extends AgentCommandTestBase{

  static String CLUSTER = "test-docker-on-yarn"

  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  private boolean testable(){
    //currently the test cases below are designed for ycloud
    //and should be disabled for general fun test
    return false;
  }

  @Test
  public void testBasicDockerApp() throws Throwable {
    if(!testable()){
      return;
    }
    String BASIC_APP_RESOURCE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/basic/resources.json"
    String BASIC_APP_META = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/basic/metainfo.json"
    String BASIC_APP_TEMPLATE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/basic/appConfig.json"

    launchDockerAppOnYarn(BASIC_APP_TEMPLATE, BASIC_APP_META, BASIC_APP_RESOURCE)
    ClusterDescription cd = execStatus(CLUSTER)
    ensureNumberOfContainersAlive("YCLOUD", 1, cd);
    ensureIpAndHostnamePresent("YCLOUD", 1, cd);
  }

  @Test
  public void testMultiCompDockerApp() throws Throwable {
    if(!testable()){
      return;
    }
    String BASIC_APP_RESOURCE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/multiComp/resources.json"
    String BASIC_APP_META = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/multiComp/metainfo.json"
    String BASIC_APP_TEMPLATE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/multiComp/appConfig.json"

    launchDockerAppOnYarn(BASIC_APP_TEMPLATE, BASIC_APP_META, BASIC_APP_RESOURCE)
    ClusterDescription cd = execStatus(CLUSTER)
    ensureNumberOfContainersAlive("YCLOUD1", 1, cd);
    ensureIpAndHostnamePresent("YCLOUD1", 1, cd);
    ensureNumberOfContainersAlive("YCLOUD2", 1, cd);
    ensureIpAndHostnamePresent("YCLOUD2", 1, cd);
  }

  @Test
  public void testMultiCompMultiContainersDockerApp() throws Throwable {
    if(!testable()){
      return;
    }
    String BASIC_APP_RESOURCE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/multiCompMultiContainer/resources.json"
    String BASIC_APP_META = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/multiCompMultiContainer/metainfo.json"
    String BASIC_APP_TEMPLATE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/multiCompMultiContainer/appConfig.json"

    launchDockerAppOnYarn(BASIC_APP_TEMPLATE, BASIC_APP_META, BASIC_APP_RESOURCE)
    //need more time for containers to come up
    sleep(10000)
    ClusterDescription cd = execStatus(CLUSTER)
    ensureNumberOfContainersAlive("YCLOUD1", 2, cd);
    ensureIpAndHostnamePresent("YCLOUD1", 2, cd);
    ensureNumberOfContainersAlive("YCLOUD2", 3, cd);
    ensureIpAndHostnamePresent("YCLOUD2", 3, cd);
  }

  @Test
  public void testOneCompFailedDockerApp() throws Throwable {
    if(!testable()){
      return;
    }
    String BASIC_APP_RESOURCE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/oneCompFailed/resources.json"
    String BASIC_APP_META = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/oneCompFailed/metainfo.json"
    String BASIC_APP_TEMPLATE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/oneCompFailed/appConfig.json"

    launchDockerAppOnYarn(BASIC_APP_TEMPLATE, BASIC_APP_META, BASIC_APP_RESOURCE)
    //need more time for containers to come up
    sleep(30000)
    ClusterDescription cd = execStatus(CLUSTER)
    print cd.toString()
    if(cd.status.get("live") != null){
      Map live = (Map)cd.status.get("live")
      if (live.get("YCLOUD1") != null){
        Map compMap = (Map)live.get("YCLOUD1")
        String[] keys = compMap.keySet().toArray()
        for(int i = 0; i < 2; i++){
          ClusterNode node = (ClusterNode)compMap.get(keys[i])
          assert node.state == StateValues.STATE_LIVE
          assert node.ip == null
          assert node.hostname == null
        }
      } else {
        fail("ycloud1 not present")
      }
    } else {
      fail("live is not in the returned state")
    }
    ensureNumberOfContainersAlive("YCLOUD2", 2, cd);
    ensureIpAndHostnamePresent("YCLOUD2", 2, cd);
  }

  @Test
  public void testAllCompFailedDockerApp() throws Throwable {
    if(!testable()){
      return;
    }
    String BASIC_APP_RESOURCE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/allCompFailed/resources.json"
    String BASIC_APP_META = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/allCompFailed/metainfo.json"
    String BASIC_APP_TEMPLATE = "../slider-core/src/test/app_packages/test_docker_on_yarn_pkg/allCompFailed/appConfig.json"

    launchDockerAppOnYarn(BASIC_APP_TEMPLATE, BASIC_APP_META, BASIC_APP_RESOURCE)
    //need more time for containers to come up
    sleep(30000)
    ClusterDescription cd = execStatus(CLUSTER)
    print cd.toString()
    if(cd.status.get("live") != null){
      Map live = (Map)cd.status.get("live")
      if (live.get("YCLOUD1") != null){
        Map compMap = (Map)live.get("YCLOUD1")
        String[] keys = compMap.keySet().toArray()
        for(int i = 0; i < 2; i++){
          ClusterNode node = (ClusterNode)compMap.get(keys[i])
          assert node.state == StateValues.STATE_LIVE
          assert node.ip == null
          assert node.hostname == null
        }
      } else {
        fail("ycloud1 not present")
      }
      if (live.get("YCLOUD2") != null){
        Map compMap = (Map)live.get("YCLOUD2")
        String[] keys = compMap.keySet().toArray()
        for(int i = 0; i < 2; i++){
          ClusterNode node = (ClusterNode)compMap.get(keys[i])
          assert node.state == StateValues.STATE_LIVE
          assert node.ip == null
          assert node.hostname == null
        }
      } else {
        fail("ycloud2 not present")
      }
    } else {
      fail("live is not in the returned state")
    }
  }

  private void launchDockerAppOnYarn(String appConfig, String metainfo, String resources) throws Throwable {
    describe("Create a cluster using metainfo, resources, and appConfig that deploy docker based application on yarn")
    assumeNotWindows()
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createSliderApplicationMinPkg(CLUSTER,
        metainfo,
        resources,
        appConfig,
        [],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    //need some time for containers to come up
    sleep(40000)
  }

  //get cluster description from yarn. make sure all containers and the app master are good
  private void ensureNumberOfContainersAlive(String compName, int numOfContainers, ClusterDescription cd){
    print cd.toString()
    if(cd.status.get("live") != null){
      Map live = (Map)cd.status.get("live")
      if (live.get(compName) != null){
        Map compMap = (Map)live.get(compName)
        String[] keys = compMap.keySet().toArray()
        assert numOfContainers == keys.length
        for(int i = 0; i < numOfContainers; i++){
          ClusterNode node = (ClusterNode)compMap.get(keys[i])
          assert node.state == StateValues.STATE_LIVE
        }
      } else {
        fail(compName + " not present")
      }
    } else {
      fail("live is not in the returned state")
    }
  }

  private void ensureIpAndHostnamePresent(String compName, int numOfContainers, ClusterDescription cd){
    print cd.toString()
    if(cd.status.get("live") != null){
      Map live = (Map)cd.status.get("live")
      if (live.get(compName) != null){
        Map compMap = (Map)live.get(compName)
        String[] keys = compMap.keySet().toArray()
        assert numOfContainers == keys.length
        for(int i = 0; i < numOfContainers; i++){
          ClusterNode node = (ClusterNode)compMap.get(keys[i])
          assert node.ip != null
          assert node.hostname != null
        }
      } else {
        fail(compName + " not present")
      }
    } else {
      fail("live is not in the returned state")
    }
  }
}
