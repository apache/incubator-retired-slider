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
import org.apache.slider.api.InternalKeys
import org.apache.slider.api.ResourceKeys
import org.apache.slider.api.RoleKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.exceptions.BadConfigException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.ConfPersister
import org.junit.Test

import static org.apache.slider.common.params.Arguments.*
import static org.apache.slider.providers.agent.AgentKeys.*

@CompileStatic
@Slf4j
class TestBuildBasicAgent extends AgentTestBase {
  static String TEST_FILES = "./src/test/resources/org/apache/slider/providers/agent/tests/"
  static File slider_core = new File(new File(".").absoluteFile, "src/test/python");
  static String bad_app_def = "appdef_1.tar"
  static File bad_app_def_path = new File(slider_core, bad_app_def)
  static String agt_conf = "agent.ini"
  static File agt_conf_path = new File(slider_core, agt_conf)

  @Override
  void checkTestAssumptions(YarnConfiguration conf) {

  }

  private File getAppDef() {
    return new File(app_def_pkg_path);
  }

  private String getAppDefURI() {
    appDef.toURI().toString()
  }

  private File getBadAppDef() {
    return bad_app_def_path;
  }

  private String getBadAppDefURI() {
    badAppDef.toURI().toString()
  }

  private File getAgentConf() {
    return agt_conf_path;
  }

  private String getAgentConfURI() {
    agentConf.toURI().toString()
  }

  private File getAgentImg() {
    return new File(app_def_pkg_path);
  }
  private String getAgentImgURI() {
    agentImg.toURI().toString()
  }


  @Test
  public void testBuildMultipleRoles() throws Throwable {

    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)
    buildAgentCluster("test_build_basic_agent_node_only",
        [(ROLE_NODE): 1],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_PACKAGE, ".",
            ARG_OPTION, APP_DEF, appDefURI,
            ARG_OPTION, AGENT_CONF, agentConfURI,
            ARG_OPTION, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_COMP_OPT, ROLE_NODE, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, ROLE_NODE, ResourceKeys.COMPONENT_PRIORITY, "1",
        ],
        true, false,
        false)

    def master = "hbase-master"
    def rs = "hbase-rs"
    ServiceLauncher<SliderClient> launcher = buildAgentCluster(clustername,
        [
            (ROLE_NODE): 1,
            (master): 1,
            (rs): 5
        ],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, PACKAGE_PATH, ".",
            ARG_OPTION, APP_DEF, appDefURI,
            ARG_OPTION, AGENT_CONF, agentConfURI,
            ARG_COMP_OPT, master, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_COMP_OPT, rs, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "2",
            ARG_RES_COMP_OPT, rs, ResourceKeys.COMPONENT_PRIORITY, "3",
            ARG_COMP_OPT, master, SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, rs, SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, master, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, rs, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, ROLE_NODE, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, ROLE_NODE, ResourceKeys.COMPONENT_PRIORITY, "1",
        ],
        true, false,
        false)
    def instanceD = launcher.service.loadPersistedClusterDescription(
        clustername)
    dumpClusterDescription("$clustername:", instanceD)
    def resource = instanceD.getResourceOperations()


    def agentComponent = resource.getMandatoryComponent(ROLE_NODE)
    agentComponent.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def masterC = resource.getMandatoryComponent(master)
    assert "2" == masterC.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def rscomponent = resource.getMandatoryComponent(rs)
    assert "5" == rscomponent.getMandatoryOption(ResourceKeys.COMPONENT_INSTANCES)

    // now create an instance with no role priority for the newnode role
    try {
      def name2 = clustername + "-2"
      buildAgentCluster(name2,
          [
              (ROLE_NODE): 2,
              "role3": 1,
              "newnode": 5
          ],
          [
              ARG_COMP_OPT, ROLE_NODE, SERVICE_NAME, "HBASE",
              ARG_COMP_OPT, "role3", SERVICE_NAME, "HBASE",
              ARG_COMP_OPT, "newnode", SERVICE_NAME, "HBASE",
              ARG_RES_COMP_OPT, "role3", ResourceKeys.COMPONENT_PRIORITY, "2",
          ],
          true, false,
          false)
      failWithBuildSucceeding(name2, "no priority for one role")
    } catch (BadConfigException expected) {
    }

    try {
      launcher = buildAgentCluster(clustername + "-10",
          [
              (ROLE_NODE): 4,
          ],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_OPTION, PACKAGE_PATH, ".",
              ARG_OPTION, APP_DEF, appDefURI,
              ARG_OPTION, AGENT_CONF, agentConfURI,
              ARG_COMP_OPT, ROLE_NODE, SCRIPT_PATH, "agent/scripts/agent.py",
              ARG_RES_COMP_OPT, ROLE_NODE, ResourceKeys.COMPONENT_PRIORITY, "1",
          ],
          true, false,
          false)
      failWithBuildSucceeding(ROLE_NODE, "too many instances")
    } catch (BadConfigException expected) {
      assert expected.message.contains("Expected minimum is 1 and maximum is 2")
      assert expected.message.contains("Component echo, yarn.component.instances value 4 out of range.")
    }
    //duplicate priorities
    try {
      def name3 = clustername + "-3"
      buildAgentCluster(name3,
          [
              (ROLE_NODE): 5,
              (master): 1,
              (rs): 5
          ],
          [
              ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "2",
              ARG_RES_COMP_OPT, rs, ResourceKeys.COMPONENT_PRIORITY, "2"
          ],

          true, false,
          false)
      failWithBuildSucceeding(name3, "duplicate priorities")
    } catch (BadConfigException expected) {
    }



    def cluster4 = clustername + "-4"

    def jvmopts = "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    buildAgentCluster(cluster4,
        [
            (master): 1,
            (rs): 5
        ],
        [
            ARG_OPTION, APP_DEF, appDefURI,
            ARG_OPTION, AGENT_CONF, agentConfURI,
            ARG_PACKAGE, ".",
            ARG_COMP_OPT, SliderKeys.COMPONENT_AM, RoleKeys.JVM_OPTS, jvmopts,
            ARG_COMP_OPT, master, RoleKeys.JVM_OPTS, jvmopts,
            ARG_COMP_OPT, rs, RoleKeys.JVM_OPTS, jvmopts,
            ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "2",
            ARG_RES_COMP_OPT, rs, ResourceKeys.COMPONENT_PRIORITY, "3",
        ],

        true, false,
        false)

    //now we want to look at the value
    AggregateConf instanceDefinition = loadInstanceDefinition(cluster4)
    def opt = instanceDefinition.getAppConfOperations().getComponentOpt(
        SliderKeys.COMPONENT_AM,
        RoleKeys.JVM_OPTS,
        "")

    assert jvmopts == opt

    // now create an instance with no component options, hence no
    // entry in the app config
    def name5 = clustername + "-5"
    buildAgentCluster(name5,
        [
            "hbase-rs": 1,
        ],
        [
            ARG_OPTION, APP_DEF, appDefURI,
            ARG_OPTION, AGENT_CONF, agentConfURI,
            ARG_PACKAGE, ".",
            ARG_RES_COMP_OPT, "hbase-rs", ResourceKeys.COMPONENT_PRIORITY, "3",
        ],
        true, false,
        false)
  }

  @Test
  public void testLabelExpressionArgs() throws Throwable {
    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)

    try {
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_OPTION, APP_DEF, appDefURI,
              ARG_RESOURCES, TEST_FILES + "good/resources_with_label.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
    } catch (BadConfigException exception) {
      log.error(
          "Build operation should not have failed with exception : \n$exception")
      fail("Build operation should not fail")
    }

    AggregateConf instanceDefinition = loadInstanceDefinition(clustername)
    def opt = instanceDefinition.getResourceOperations().getComponentOpt(
        "echo",
        ResourceKeys.YARN_LABEL_EXPRESSION,
        null)
    assert opt == null, "Expect null"

    opt = instanceDefinition.getResourceOperations().getComponentOpt(
        "hbase-master",
        ResourceKeys.YARN_LABEL_EXPRESSION,
        null)
    assert opt == "", "Expect empty string"

    opt = instanceDefinition.getResourceOperations().getComponentOpt(
        "hbase-rs",
        ResourceKeys.YARN_LABEL_EXPRESSION,
        null)
    assert opt == "coquelicot && amaranth", "Expect colors you have not heard of"

    def label = instanceDefinition.getInternalOperations().get(
        InternalKeys.INTERNAL_QUEUE)
    assert label == null, "Default queue expected"
  }

  @Test
  public void testUpdateBasicAgent() throws Throwable {

    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)
    
    def master = "hbase-master"
    def rs = "hbase-rs"
    ServiceLauncher<SliderClient> launcher = buildAgentCluster(clustername,
        [
            (ROLE_NODE): 2,
            (master): 1,
            (rs): 5
        ],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, PACKAGE_PATH, ".",
            ARG_OPTION, APP_DEF, appDefURI,
            ARG_OPTION, AGENT_CONF, agentConfURI,
            ARG_COMP_OPT, master, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_COMP_OPT, rs, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "2",
            ARG_RES_COMP_OPT, rs, ResourceKeys.COMPONENT_PRIORITY, "3",
            ARG_COMP_OPT, master, SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, rs, SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, master, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, rs, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, ROLE_NODE, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, ROLE_NODE, ResourceKeys.COMPONENT_PRIORITY, "1",
        ],
        true, false,
        false)
    def instanceD = launcher.service.loadPersistedClusterDescription(
        clustername)
    dumpClusterDescription("$clustername:", instanceD)
    def resource = instanceD.getResourceOperations()

    def agentComponent = resource.getMandatoryComponent(ROLE_NODE)
    agentComponent.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def masterC = resource.getMandatoryComponent(master)
    assert "2" == masterC.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def rscomponent = resource.getMandatoryComponent(rs)
    assert "5" == rscomponent.getMandatoryOption(ResourceKeys.COMPONENT_INSTANCES)

    // change master priority and rs instances through update action
    ServiceLauncher<SliderClient> launcher2 = updateAgentCluster(clustername,
        [
            (ROLE_NODE): 2,
            (master): 1,
            (rs): 6
        ],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, PACKAGE_PATH, ".",
            ARG_OPTION, APP_DEF, appDefURI,
            ARG_OPTION, AGENT_CONF, agentConfURI,
            ARG_COMP_OPT, master, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_COMP_OPT, rs, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, master, ResourceKeys.COMPONENT_PRIORITY, "4",
            ARG_RES_COMP_OPT, rs, ResourceKeys.COMPONENT_PRIORITY, "3",
            ARG_COMP_OPT, master, SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, rs, SERVICE_NAME, "HBASE",
            ARG_COMP_OPT, master, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, rs, AgentKeys.APP_HOME, "/share/hbase/hbase-0.96.1-hadoop2",
            ARG_COMP_OPT, ROLE_NODE, SCRIPT_PATH, "agent/scripts/agent.py",
            ARG_RES_COMP_OPT, ROLE_NODE, ResourceKeys.COMPONENT_PRIORITY, "1",
        ],
        true)
    def instanceDef = launcher.service.loadPersistedClusterDescription(
        clustername)
    dumpClusterDescription("$clustername:", instanceDef)
    def resource2 = instanceDef.getResourceOperations()

    def agentComponent2 = resource2.getMandatoryComponent(ROLE_NODE)
    agentComponent2.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def masterC2 = resource2.getMandatoryComponent(master)
    assert "4" == masterC2.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY)

    def rscomponent2 = resource2.getMandatoryComponent(rs)
    assert "6" == rscomponent2.getMandatoryOption(ResourceKeys.COMPONENT_INSTANCES)
  }
  
  public AggregateConf loadInstanceDefinition(String name) {
    def cluster4
    def sliderFS = createSliderFileSystem()
    def dirPath = sliderFS.buildClusterDirPath(name)
    ConfPersister persister = new ConfPersister(sliderFS, dirPath)
    AggregateConf instanceDefinition = new AggregateConf();
    persister.load(instanceDefinition)
    return instanceDefinition
  }

  @Test
  public void testGoodAgentArgs() throws Throwable {
    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)

    try {
      def badArgs1 = "test_good_agent_args-1"
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_OPTION, APP_DEF, appDefURI,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
    } catch (BadConfigException exception) {
      log.error(
          "Build operation should not have failed with exception : \n$exception")
      fail("Build operation should not fail")
    }
  }

  @Test
  public void testBadAgentArgs_Unknown_Component() throws Throwable {
    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)

    try {
      def badArgs1 = "test_bad_agent_unk_comp"
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_OPTION, APP_DEF, appDefURI,
              ARG_RESOURCES, TEST_FILES + "bad/resources-3.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "bad component type node")
    } catch (BadConfigException expected) {
      assertExceptionDetails(expected, SliderExitCodes.EXIT_BAD_CONFIGURATION,
        "Component node is not a member of application")
    }
  }

  @Test
  public void testSubmitToSpecificQueue() throws Throwable {
    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)

    try {
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_OPTION, APP_DEF, appDefURI,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json",
              ARG_QUEUE, "labeled"
          ],
          true, false,
          false)
    } catch (BadConfigException exception) {
      log.error(
          "Build operation should not have failed with exception : \n$exception")
      fail("Build operation should not fail")
    }

    AggregateConf instanceDefinition = loadInstanceDefinition(clustername)
    def label = instanceDefinition.getInternalOperations().get(
        InternalKeys.INTERNAL_QUEUE)
    assert label == "labeled", "Expect labeled as the queue"
  }

  @Test
  public void testBadAgentArgs() throws Throwable {
    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)

    try {
      def badArgs1 = "test_bad_agent_args-2"
      buildAgentCluster(badArgs1,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_IMAGE, agentImgURI,
              ARG_OPTION, APP_DEF, appDefURI,
              ARG_OPTION, AGENT_CONF, agentConfURI,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "both app image path and home dir was provided")
    } catch (BadConfigException expected) {
      log.info("Expected failure.", expected)
      assert expected.message.contains("Both application image path and home dir have been provided")
    }

    try {
      def badArgs1 = "test_bad_agent_args-3"
      buildAgentCluster(badArgs1,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_OPTION, AGENT_CONF, agentConfURI,
              ARG_PACKAGE, ".",
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "missing app def file")
    } catch (BadConfigException expected) {
      log.info("Expected failure.", expected)
      assert expected.message.contains("Application definition must be provided. Missing option application.def")
    }

    try {
      def badArgs1 = "test_bad_agent_args-6"
      buildAgentCluster(badArgs1,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_OPTION, AGENT_CONF, agentConfURI,
              ARG_PACKAGE, ".",
              ARG_OPTION, APP_DEF, badAppDefURI,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "bad app def file")
    } catch (BadConfigException expected) {
      log.info("Expected failure.", expected)
      assert expected.message.contains("App definition must be packaged as a .zip file")
    }
  }

  @Test
  public void testTemplateArgs() throws Throwable {

    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)
    buildAgentCluster("test_build_template_args_good",
        [:],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, APP_DEF, appDefURI,
            ARG_OPTION, AGENT_CONF, agentConfURI,
            ARG_PACKAGE, ".",
            ARG_RESOURCES, TEST_FILES + "good/resources.json",
            ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
        ],
        true, false,
        false)
  }


  @Test
  public void testBadTemplates() throws Throwable {

    String clustername = createMiniCluster(
        "",
        configuration,
        1,
        1,
        1,
        true,
        false)

    try {


      def badArgs1 = "test_build_template_args_bad-1"
      buildAgentCluster(badArgs1,
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_RESOURCES, TEST_FILES + "bad/appconf-1.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "bad resource template")
    } catch (BadConfigException expected) {
    }

    try {

      def bad2 = "test_build_template_args_bad-2"
      buildAgentCluster(bad2,
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_TEMPLATE, TEST_FILES + "bad/appconf-1.json",
          ],
          true, false,
          false)

      failWithBuildSucceeding(bad2, "a bad app conf")
    } catch (BadConfigException expected) {
    }

    try {

      def bad3 = "test_build_template_args_bad-3"
      buildAgentCluster(bad3,
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_TEMPLATE, "unknown.json",
          ],
          true, false,
          false)
      failWithBuildSucceeding(bad3, "missing template file")
    } catch (BadConfigException expected) {
    }


    try {

      def bad4 = "test_build_template_args_bad-4"
      buildAgentCluster(bad4,
          [:],
          [

              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_TEMPLATE, TEST_FILES + "bad/appconf-2.json",
          ],
          true, false,
          false)

      failWithBuildSucceeding(bad4, "Unparseable JSON")
    } catch (BadConfigException expected) {
    }

  }

  public void failWithBuildSucceeding(String name, String reason) {
    def badArgs1
    AggregateConf instanceDefinition = loadInstanceDefinition(name)
    log.error(
        "Build operation should have failed from $reason : \n$instanceDefinition")
    fail("Build operation should have failed from $reason")
  }


}
