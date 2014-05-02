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
import org.apache.slider.api.RoleKeys
import org.apache.slider.client.SliderClient
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

  @Override
  void checkTestAssumptions(YarnConfiguration conf) {

  }

  private static class TestResources {
    static File slider_core = new File(new File(".").absoluteFile, "src/test/python");
    static String app_def = "appdef_1.tar"
    static File app_def_path = new File(slider_core, app_def)
    static String agt_conf = "agent.ini"
    static File agt_conf_path = new File(slider_core, agt_conf)

    static public File getAppDef() {
      return app_def_path;
    }

    static public File getAgentConf() {
      return agt_conf_path;
    }

    static public File getAgentImg() {
      return app_def_path;
    }
  }

  @Test
  public void testBuildMultipleRoles() throws Throwable {

    def clustername = "test_build_basic_agent"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)
    buildAgentCluster("test_build_basic_agent_node_only",
        [(ROLE_NODE): 5],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_PACKAGE, ".",
            ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
            ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
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
            (ROLE_NODE): 5,
            (master): 1,
            (rs): 5
        ],
        [
            ARG_OPTION, CONTROLLER_URL, "http://localhost",
            ARG_OPTION, PACKAGE_PATH, ".",
            ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
            ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
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
              (ROLE_NODE): 5,
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
            ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
            ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
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
            "role": 1,
        ],
        [
            ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
            ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
            ARG_PACKAGE, ".",
            ARG_RES_COMP_OPT, "role", ResourceKeys.COMPONENT_PRIORITY, "3",
        ],
        true, false,
        false)
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
  public void testBadAgentArgs() throws Throwable {
    def clustername = "test_bad_template_args"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)

    try {
      def badArgs1 = "test_bad_agent_args-1"
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
              ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "missing package home or image path")
    } catch (BadConfigException expected) {
     
    }

    try {
      def badArgs1 = "test_bad_agent_args-2"
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_IMAGE, "file://" + TestResources.getAgentImg().absolutePath + ".badfile",
              ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
              ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "bad image path")
    } catch (BadConfigException expected) {
    }

    try {
      def badArgs1 = "test_bad_agent_args-3"
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "bad app def file")
    } catch (BadConfigException expected) {
    }

    try {
      def badArgs1 = "test_bad_agent_args-5"
      buildAgentCluster(clustername,
          [:],
          [
              ARG_OPTION, CONTROLLER_URL, "http://localhost",
              ARG_PACKAGE, ".",
              ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
              ARG_RESOURCES, TEST_FILES + "good/resources.json",
              ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
          ],
          true, false,
          false)
      failWithBuildSucceeding(badArgs1, "bad agent conf file")
    } catch (BadConfigException expected) {
    }
  }

  @Test
  public void testTemplateArgs() throws Throwable {


    def clustername = "test_build_template_args"
    createMiniCluster(
        clustername,
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
            ARG_OPTION, APP_DEF, "file://" + TestResources.getAppDef().absolutePath,
            ARG_OPTION, AGENT_CONF, "file://" + TestResources.getAgentConf().absolutePath,
            ARG_PACKAGE, ".",
            ARG_RESOURCES, TEST_FILES + "good/resources.json",
            ARG_TEMPLATE, TEST_FILES + "good/appconf.json"
        ],
        true, false,
        false)
  }


  @Test
  public void testBadTemplates() throws Throwable {


    def clustername = "test_bad_template_args"
    createMiniCluster(
        clustername,
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
