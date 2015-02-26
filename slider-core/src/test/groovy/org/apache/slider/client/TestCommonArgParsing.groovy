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

package org.apache.slider.client

import com.google.common.collect.Maps
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.api.ResourceKeys
import org.apache.slider.api.RoleKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.*
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.exceptions.ErrorStrings
import org.junit.Assert
import org.junit.Test

/**
 * Test handling of common arguments, specifically how things get split up
 */
@CompileStatic
@Slf4j

class TestCommonArgParsing implements SliderActions, Arguments {


  public static final String CLUSTERNAME = "clustername"

  @Test
  public void testCreateActionArgs() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ACTION_CREATE, 'cluster1'])
    assert clientArgs.clusterName == 'cluster1'
  }

  @Test
  public void testCreateFailsNoClustername() throws Throwable {
    assertParseFails([ACTION_CREATE])
  }

  @Test
  public void testCreateFailsTwoClusternames() throws Throwable {
    assertParseFails([
        ACTION_CREATE,
        "c1",
        "c2",
      ])
  }

  @Test
  public void testHelp() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ACTION_HELP])
    assert clientArgs.clusterName == null
  }

  @Test
  public void testSliderBasePath() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ACTION_LIST,
        ARG_BASE_PATH,  "/projects/slider/clusters"])
    assert clientArgs.basePath == new Path("/projects/slider/clusters")
  }

  @Test
  public void testNoSliderBasePath() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ACTION_LIST])
    assert clientArgs.basePath == null
  }

  @Test
  public void testListNoClusternames() throws Throwable {
    ClientArgs clientArgs = createClientArgs([ACTION_LIST])
    assert clientArgs.clusterName == null
  }

  @Test
  public void testListNoClusternamesDefinition() throws Throwable {
    ClientArgs clientArgs = createClientArgs(
        [ACTION_LIST,
        ARG_DEFINE,
        'fs.default.FS=file://localhost',
        ])
    assert clientArgs.clusterName == null
  }

  @Test
  public void testList1Clustername() throws Throwable {
    ClientArgs ca = createClientArgs([ACTION_LIST, 'cluster1'])
    assert ca.clusterName == 'cluster1'
    assert ca.coreAction instanceof ActionListArgs
  }

  @Test
  public void testListFailsTwoClusternames() throws Throwable {
    assertParseFails([
        ACTION_LIST,
        "c1",
        "c2",
      ])
  }

  @Test
  public void testDefinitions() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_CREATE,
        CLUSTERNAME,
        "-D","yarn.resourcemanager.principal=yarn/server@LOCAL",
        "-D","dfs.datanode.kerberos.principal=hdfs/server@LOCAL",
    ])
    Configuration conf = new Configuration(false)
    ca.applyDefinitions(conf)
    assert ca.clusterName == CLUSTERNAME
    assert conf.get(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH) == null
    SliderUtils.verifyPrincipalSet(conf, YarnConfiguration.RM_PRINCIPAL);
    SliderUtils.verifyPrincipalSet(
        conf,
        DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY);

  }

  @Test
  public void testDefinitionsSettingBaseSliderDir() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_CREATE,
        CLUSTERNAME,
        "--basepath", "/projects/slider/clusters",
        "-D","yarn.resourcemanager.principal=yarn/server@LOCAL",
        "-D","dfs.datanode.kerberos.principal=hdfs/server@LOCAL",
    ])
    Configuration conf = new Configuration(false)
    ca.applyDefinitions(conf)
    assert ca.clusterName == CLUSTERNAME
    assert conf.get(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH) == "/projects/slider/clusters"
    SliderUtils.verifyPrincipalSet(conf, YarnConfiguration.RM_PRINCIPAL);
    SliderUtils.verifyPrincipalSet(conf, DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY);

  }

  /**
   * Test a start command
   * @throws Throwable
   */
  @Test
  public void testComplexThaw() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_THAW,
        "--manager", "rhel:8032", "--filesystem", "hdfs://rhel:9090",
        "-S","java.security.krb5.realm=LOCAL","-S", "java.security.krb5.kdc=rhel",
        "-D","yarn.resourcemanager.principal=yarn/rhel@LOCAL",
        "-D","namenode.resourcemanager.principal=hdfs/rhel@LOCAL",
        "cl1"    
    ])
    assert "cl1" == ca.clusterName
    assert ca.coreAction instanceof ActionThawArgs

  }
  
  /**
   * Test a force kill command where the app comes at the end of the line
   * @throws Throwable
   * 
   */
  @Test
  public void testStatusSplit() throws Throwable {

    String appId = "application_1381252124398_0013"
    ClientArgs ca = createClientArgs([
        ACTION_STATUS,
        "--manager", "rhel:8032",
        "--filesystem", "hdfs://rhel:9090",
        "-S","java.security.krb5.realm=LOCAL",
        "-S", "java.security.krb5.kdc=rhel",
        "-D","yarn.resourcemanager.principal=yarn/rhel@LOCAL",
        "-D","namenode.resourcemanager.principal=hdfs/rhel@LOCAL",
        appId
    ])
    assert appId == ca.clusterName
  }
  
  @Test
  public void testFreezeFailsNoArg() throws Throwable {
    assertParseFails([
        ACTION_FREEZE,
    ])
  }
  
  @Test
  public void testFreezeWorks1Arg() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_FREEZE,
        CLUSTERNAME,
    ])
    assert ca.clusterName == CLUSTERNAME
    assert ca.coreAction instanceof ActionFreezeArgs
  }
  
  @Test
  public void testFreezeFails2Arg() throws Throwable {
    assertParseFails([
        ACTION_FREEZE, "cluster", "cluster2"
    ])
  }

  @Test
  public void testFreezeForceWaitAndMessage() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_FREEZE, CLUSTERNAME,
        ARG_FORCE,
        ARG_WAIT, "0",
        ARG_MESSAGE, "explanation"
    ])
    assert ca.clusterName == CLUSTERNAME
    assert ca.coreAction instanceof ActionFreezeArgs
    ActionFreezeArgs freezeArgs = (ActionFreezeArgs) ca.coreAction;
    assert freezeArgs.message == "explanation"
    assert freezeArgs.force;
  }

  @Test
  public void testGetStatusWorks1Arg() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_STATUS,
        CLUSTERNAME,
    ])
    assert ca.clusterName == CLUSTERNAME
    assert ca.coreAction instanceof ActionStatusArgs
  }
  
  @Test
  public void testExistsWorks1Arg() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_EXISTS,
        CLUSTERNAME,
        ARG_LIVE
    ])
    assert ca.clusterName == CLUSTERNAME
    assert ca.coreAction instanceof ActionExistsArgs
    assert ca.actionExistsArgs.live
  }  

  @Test
  public void testDestroy1Arg() throws Throwable {
    ClientArgs ca = createClientArgs([
        ACTION_DESTROY,
        CLUSTERNAME,
    ])
    assert ca.clusterName == CLUSTERNAME
    assert ca.coreAction instanceof ActionDestroyArgs
  }
  
  /**
   * Assert that a pass fails with a BadCommandArgumentsException
   * @param argsList
   */
  
  private void assertParseFails(List argsList) {
    try {
      ClientArgs clientArgs = createClientArgs(argsList)
      Assert.fail("exected an exception, got $clientArgs")
    } catch (BadCommandArgumentsException ignored) {
      //expected
    }
  }
  
  /**
   * build and parse client args, after adding the base args list
   * @param argsList
   */
  public ClientArgs createClientArgs(List argsList) {
    def serviceArgs = new ClientArgs(argsList + baseArgs())
    serviceArgs.parse()
    serviceArgs
  }
  
  public ActionCreateArgs createAction(List argsList) {
    def ca = createClientArgs(argsList)
    assert ca.action == ACTION_CREATE
    ActionCreateArgs args = ca.actionCreateArgs
    assert args != null
    return args
  }

  /**
   * build the list of base arguments for all operations
   * @return the base arguments
   */
  private List<String> baseArgs() {
    return [

    ]
  }

  /**
   * Here to diagnose some problems with groovy concatenation on
   * some groovy versions
   * @throws Throwable
   */
  @Test
  public void testListConcat() throws Throwable {
    def l1 = ["a"] + baseArgs()
    assert l1.size() == 1
  }

  @Test
  public void testCreateWaitTime() throws Throwable {
    ActionCreateArgs createArgs = createAction([
        ACTION_CREATE, 'cluster1',
        ARG_WAIT, "600"
    ])
    assert 600 == createArgs.getWaittime()
  }


  @Test
  public void testSingleRoleArg() throws Throwable {
    def createArgs = createAction([
        ACTION_CREATE, 'cluster1',
        ARG_COMPONENT,"master","5",
    ])
    def tuples = createArgs.componentTuples;
    assert tuples.size() == 2;
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assert roleMap["master"] == "5"
  }
  
  @Test
  public void testNoRoleArg() throws Throwable {
    ActionCreateArgs createArgs = createAction([
        ACTION_CREATE, 'cluster1',
    ])
    def tuples = createArgs.componentTuples;
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assert roleMap["master"] == null
  }


  @Test
  public void testMultiRoleArgBuild() throws Throwable {
    def ca = createClientArgs([
        ACTION_BUILD, 'cluster1',
        ARG_COMPONENT, "master", "1",
        ARG_COMPONENT, "worker", "2",
    ])
    assert ca.action == ACTION_BUILD
    assert ca.coreAction instanceof ActionBuildArgs
    assert ca.buildingActionArgs instanceof ActionBuildArgs
    AbstractClusterBuildingActionArgs args = ca.actionBuildArgs
    def tuples = args.componentTuples;
    assert tuples.size() == 4;
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assert roleMap["master"] == "1"
    assert roleMap["worker"] == "2"
  }
  
  @Test
  public void testArgUpdate() throws Throwable {
    def ca = createClientArgs([
        ACTION_UPDATE, 'cluster1',
        ARG_TEMPLATE, "appConfig.json",
    ])
    assert ca.action == ACTION_UPDATE
    assert ca.coreAction instanceof ActionUpdateArgs
    assert ca.actionUpdateArgs instanceof ActionUpdateArgs
    AbstractClusterBuildingActionArgs args = ca.actionUpdateArgs
    assert args.template != null
  }
  
  @Test
  public void testFlexArgs() throws Throwable {
    def ca = createClientArgs([
        ACTION_FLEX, 'cluster1',
        ARG_COMPONENT, "master", "1",
        ARG_COMPONENT, "worker", "2",
    ])
    assert ca.coreAction instanceof ActionFlexArgs
    def tuples = ca.actionFlexArgs.componentTuples;
    assert tuples.size() == 4;
    Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
    assert roleMap["master"] == "1"
    assert roleMap["worker"] == "2"
  }

  @Test
  public void testDuplicateRole() throws Throwable {
    ActionCreateArgs createArgs = createAction([
        ACTION_CREATE, 'cluster1',
        ARG_COMPONENT, "master", "1",
        ARG_COMPONENT, "master", "2",
    ])
    def tuples = createArgs.componentTuples;
    assert tuples.size() == 4;
    try {
      Map<String, String> roleMap = ArgOps.convertTupleListToMap(
          "roles",
          tuples);
      Assert.fail("got a role map $roleMap not a failure");
    } catch (BadCommandArgumentsException expected) {
      assert expected.message.contains(ErrorStrings.ERROR_DUPLICATE_ENTRY)
    }
  }
     
  @Test
  public void testOddRoleCount() throws Throwable {
    ActionCreateArgs createArgs = createAction([
        ACTION_CREATE, 'cluster1',
        ARG_COMPONENT,"master","1",
        ARG_COMPONENT,"master","2",
    ])
    List<String> tuples = createArgs.componentTuples
    tuples += "loggers";
    assert tuples.size() == 5;
    try {
      Map<String, String> roleMap = ArgOps.convertTupleListToMap("roles", tuples);
      Assert.fail("got a role map $roleMap not a failure");
    } catch (BadCommandArgumentsException expected) {
      assert expected.message.contains(ErrorStrings.ERROR_PARSE_FAILURE)
    }
  }

  /**
   * Create some role-opt client args, so that multiple tests can use it 
   * @return the args
   */
  public ActionCreateArgs createRoleOptClientArgs() {
    ActionCreateArgs createArgs = createAction([
        ACTION_CREATE, 'cluster1',
        ARG_COMPONENT, "master", "1",
        ARG_COMP_OPT, "master", "cheese", "swiss",
        ARG_COMP_OPT, "master", "env.CHEESE", "cheddar",
        ARG_COMP_OPT, "master", ResourceKeys.YARN_CORES, 3,

        ARG_COMPONENT, "worker", "2",
        ARG_COMP_OPT, "worker", ResourceKeys.YARN_CORES, 2,
        ARG_COMP_OPT, "worker", RoleKeys.JVM_HEAP, "65536",
        ARG_COMP_OPT, "worker", "env.CHEESE", "stilton",
    ])
    return createArgs
  }

  @Test
  public void testRoleOptionParse() throws Throwable {
    ActionCreateArgs createArgs = createRoleOptClientArgs()
    def tripleMaps = createArgs.compOptionMap
    def workerOpts = tripleMaps["worker"];
    assert workerOpts.size() == 3
    assert workerOpts[ResourceKeys.YARN_CORES] == "2"
    assert workerOpts[RoleKeys.JVM_HEAP] == "65536"
    
    def masterOpts = tripleMaps["master"];
    assert masterOpts.size() == 3
    assert masterOpts[ResourceKeys.YARN_CORES] == "3"

  }

  @Test
  public void testRoleOptionsMerge() throws Throwable {
    ActionCreateArgs createArgs = createRoleOptClientArgs()

    def roleOpts = createArgs.compOptionMap

    def clusterRoleMap = createEnvMap()
    SliderUtils.applyCommandLineRoleOptsToRoleMap(clusterRoleMap, roleOpts);

    def masterOpts = clusterRoleMap["master"];
    assert masterOpts["cheese"] == "swiss"

    def workerOpts = clusterRoleMap["worker"];
    assert workerOpts["env.CHEESE"] == "stilton"
  }

  @Test
  public void testEnvVariableApply() throws Throwable {
    ActionCreateArgs createArgs = createRoleOptClientArgs()

    
    def roleOpts = createArgs.compOptionMap

    Map<String, Map<String, String>> clusterRoleMap = createEnvMap()
    SliderUtils.applyCommandLineRoleOptsToRoleMap(clusterRoleMap, roleOpts);

    def workerOpts = Maps.newHashMap(clusterRoleMap["worker"])
    assert workerOpts["env.CHEESE"] == "stilton";

    Map<String, String> envmap = SliderUtils.buildEnvMap(workerOpts);
    assert envmap["CHEESE"] == "stilton";

  }

  /**
   * static compiler complaining about matching LinkedHashMap with Map,
   * so some explicit creation here
   * @return a map of maps
   */
  public Map<String, Map<String, String>> createEnvMap() {

    Map<String, String> cheese = new HashMap<>()
    cheese["cheese"]="french"
    Map<String, String> envCheese = new HashMap<>()
    envCheese["env.CHEESE"] = "french"
    Map<String, Map<String, String>> envMap = new HashMap<>()
    envMap["master"] = cheese
    envMap["worker"] = envCheese
    return envMap
  }


}
