/**
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

package org.apache.slider.providers.agent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.ExportEntry;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.agent.application.metadata.Application;
import org.apache.slider.providers.agent.application.metadata.CommandOrder;
import org.apache.slider.providers.agent.application.metadata.CommandScript;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.ComponentCommand;
import org.apache.slider.providers.agent.application.metadata.ComponentExport;
import org.apache.slider.providers.agent.application.metadata.ConfigFile;
import org.apache.slider.providers.agent.application.metadata.DefaultConfig;
import org.apache.slider.providers.agent.application.metadata.Export;
import org.apache.slider.providers.agent.application.metadata.ExportGroup;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.MetainfoParser;
import org.apache.slider.providers.agent.application.metadata.PropertyInfo;
import org.apache.slider.server.appmaster.model.mock.MockRegistryOperations;
import org.apache.slider.server.appmaster.model.mock.MockApplicationAttemptId;
import org.apache.slider.server.appmaster.model.mock.MockApplicationId;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.apache.slider.server.appmaster.model.mock.MockFileSystem;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentCommandType;
import org.apache.slider.server.appmaster.web.rest.agent.CommandReport;
import org.apache.slider.server.appmaster.web.rest.agent.ComponentStatus;
import org.apache.slider.server.appmaster.web.rest.agent.ExecutionCommand;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;


public class TestAgentProviderService {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAgentProviderService.class);
  private static final String metainfo_1_str = "<metainfo>\n"
                                               + "  <schemaVersion>2.0</schemaVersion>\n"
                                               + "  <application>\n"
                                               + "      <name>HBASE</name>\n"
                                               + "      <comment>\n"
                                               + "        Apache HBase\n"
                                               + "      </comment>\n"
                                               + "      <version>0.96.0.2.1.1</version>\n"
                                               + "      <type>YARN-APP</type>\n"
                                               + "      <minHadoopVersion>2.1.0</minHadoopVersion>\n"
                                               + "      <exportedConfigs>hbase-site,global</exportedConfigs>\n"
                                               + "      <exportGroups>\n"
                                               + "        <exportGroup>\n"
                                               + "          <name>QuickLinks</name>\n"
                                               + "          <exports>\n"
                                               + "            <export>\n"
                                               + "              <name>JMX_Endpoint</name>\n"
                                               + "              <value>http://${HBASE_MASTER_HOST}:${site.hbase-site.hbase.master.info.port}/jmx</value>\n"
                                               + "            </export>\n"
                                               + "            <export>\n"
                                               + "              <name>Master_Status</name>\n"
                                               + "              <value>http://${HBASE_MASTER_HOST}:${site.hbase-site.hbase.master.info.port}/master-status</value>\n"
                                               + "            </export>\n"
                                               + "            <export>\n"
                                               + "              <name>Comp_Endpoint</name>\n"
                                               + "              <value>http://${HBASE_REGIONSERVER_HOST}:${site.global.listen_port}</value>\n"
                                               + "            </export>\n"
                                               + "          </exports>\n"
                                               + "        </exportGroup>\n"
                                               + "      </exportGroups>\n"
                                               + "      <commandOrders>\n"
                                               + "        <commandOrder>\n"
                                               + "          <command>HBASE_REGIONSERVER-START</command>\n"
                                               + "          <requires>HBASE_MASTER-STARTED</requires>\n"
                                               + "        </commandOrder>\n"
                                               + "        <commandOrder>\n"
                                               + "          <command>A-START</command>\n"
                                               + "          <requires>B-STARTED</requires>\n"
                                               + "        </commandOrder>\n"
                                               + "      </commandOrders>\n"
                                               + "      <components>\n"
                                               + "        <component>\n"
                                               + "          <name>HBASE_REST</name>\n"
                                               + "          <category>MASTER</category>\n"
                                               + "          <commandScript>\n"
                                               + "            <script>scripts/hbase_rest.py</script>\n"
                                               + "            <scriptType>PYTHON</scriptType>\n"
                                               + "            <timeout>600</timeout>\n"
                                               + "          </commandScript>\n"
                                               + "        </component>\n"
                                               + "        <component>\n"
                                               + "          <name>HBASE_MASTER</name>\n"
                                               + "          <category>MASTER</category>\n"
                                               + "          <publishConfig>true</publishConfig>\n"
                                               + "          <autoStartOnFailure>true</autoStartOnFailure>\n"
                                               + "          <appExports>QuickLinks-JMX_Endpoint,QuickLinks-Master_Status</appExports>\n"
                                               + "          <compExports>QuickLinks-Comp_Endpoint</compExports>\n"
                                               + "          <minInstanceCount>1</minInstanceCount>\n"
                                               + "          <maxInstanceCount>2</maxInstanceCount>\n"
                                               + "          <commandScript>\n"
                                               + "            <script>scripts/hbase_master.py</script>\n"
                                               + "            <scriptType>PYTHON</scriptType>\n"
                                               + "            <timeout>600</timeout>\n"
                                               + "          </commandScript>\n"
                                               + "        </component>\n"
                                               + "        <component>\n"
                                               + "          <name>HBASE_REGIONSERVER</name>\n"
                                               + "          <category>SLAVE</category>\n"
                                               + "          <minInstanceCount>1</minInstanceCount>\n"
                                               + "          <autoStartOnFailure>Falsee</autoStartOnFailure>\n"
                                               + "          <commandScript>\n"
                                               + "            <script>scripts/hbase_regionserver.py</script>\n"
                                               + "            <scriptType>PYTHON</scriptType>\n"
                                               + "          </commandScript>\n"
                                               + "          <compExports>QuickLinks-Comp_Endpoint</compExports>\n"
                                               + "          <componentExports>\n"
                                               + "            <componentExport>\n"
                                               + "              <name>PropertyA</name>\n"
                                               + "              <value>${THIS_HOST}:${site.global.listen_port}</value>\n"
                                               + "            </componentExport>\n"
                                               + "            <componentExport>\n"
                                               + "              <name>PropertyB</name>\n"
                                               + "              <value>AConstant</value>\n"
                                               + "            </componentExport>\n"
                                               + "          </componentExports>\n"
                                               + "        </component>\n"
                                               + "      </components>\n"
                                               + "      <osSpecifics>\n"
                                               + "        <osSpecific>\n"
                                               + "          <osType>any</osType>\n"
                                               + "          <packages>\n"
                                               + "            <package>\n"
                                               + "              <type>tarball</type>\n"
                                               + "              <name>files/hbase-0.96.1-hadoop2-bin.tar.gz</name>\n"
                                               + "            </package>\n"
                                               + "          </packages>\n"
                                               + "        </osSpecific>\n"
                                               + "      </osSpecifics>\n"
                                               + "      <configFiles>\n"
                                               + "        <configFile>\n"
                                               + "          <type>xml</type>\n"
                                               + "          <fileName>hbase-site.xml</fileName>\n"
                                               + "          <dictionaryName>hbase-site</dictionaryName>\n"
                                               + "        </configFile>\n"
                                               + "        <configFile>\n"
                                               + "          <type>env</type>\n"
                                               + "          <fileName>hbase-env.sh</fileName>\n"
                                               + "          <dictionaryName>hbase-env</dictionaryName>\n"
                                               + "        </configFile>\n"
                                               + "      </configFiles>\n"
                                               + "  </application>\n"
                                               + "</metainfo>";
  private static final String metainfo_2_str = "<metainfo>\n"
                                               + "  <schemaVersion>2.0</schemaVersion>\n"
                                               + "  <application>\n"
                                               + "      <name>HBASE</name>\n"
                                               + "      <comment>\n"
                                               + "        Apache HBase\n"
                                               + "      </comment>\n"
                                               + "      <version>0.96.0.2.1.1</version>\n"
                                               + "      <type>YARN-APP</type>\n"
                                               + "      <minHadoopVersion>2.1.0</minHadoopVersion>\n"
                                               + "      <components>\n"
                                               + "        <component>\n"
                                               + "          <name>HBASE_MASTER</name>\n"
                                               + "          <category>MASTER</category>\n"
                                               + "          <publishConfig>true</publishConfig>\n"
                                               + "          <minInstanceCount>1</minInstanceCount>\n"
                                               + "          <maxInstanceCount>2</maxInstanceCount>\n"
                                               + "          <commandScript>\n"
                                               + "            <script>scripts/hbase_master.py</script>\n"
                                               + "            <scriptType>PYTHON</scriptType>\n"
                                               + "            <timeout>600</timeout>\n"
                                               + "          </commandScript>\n"
                                               + "        </component>\n"
                                               + "        <component>\n"
                                               + "          <name>HBASE_REGIONSERVER</name>\n"
                                               + "          <category>SLAVE</category>\n"
                                               + "          <minInstanceCount>1</minInstanceCount>\n"
                                               + "          <commandScript>\n"
                                               + "            <script>scripts/hbase_regionserver.py</script>\n"
                                               + "            <scriptType>PYTHON</scriptType>\n"
                                               + "          </commandScript>\n"
                                               + "        </component>\n"
                                               + "      </components>\n"
                                               + "  </application>\n"
                                               + "</metainfo>";

  @Test
  public void testRegistration() throws Exception {

    ConfTree tree = new ConfTree();
    tree.global.put(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");

    AgentProviderService aps = createAgentProviderService(new Configuration());
    ContainerLaunchContext ctx = createNiceMock(ContainerLaunchContext.class);
    AggregateConf instanceDefinition = new AggregateConf();

    instanceDefinition.setInternal(tree);
    instanceDefinition.setAppConf(tree);
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.APP_DEF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_CONF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_VERSION, ".");

    Container container = createNiceMock(Container.class);
    String role = "HBASE_MASTER";
    SliderFileSystem sliderFileSystem = createNiceMock(SliderFileSystem.class);
    ContainerLauncher launcher = createNiceMock(ContainerLauncher.class);
    Path generatedConfPath = new Path(".", "test");
    MapOperations resourceComponent = new MapOperations();
    MapOperations appComponent = new MapOperations();
    Path containerTmpDirPath = new Path(".", "test");
    FileSystem mockFs = createNiceMock(FileSystem.class);
    expect(mockFs.exists(anyObject(Path.class))).andReturn(true);
    expect(sliderFileSystem.getFileSystem())
        .andReturn(mockFs).anyTimes();
    expect(sliderFileSystem.createAmResource(anyObject(Path.class),
                                             anyObject(LocalResourceType.class)))
        .andReturn(createNiceMock(LocalResource.class)).anyTimes();
    expect(container.getId()).andReturn(new MockContainerId(1)).anyTimes();
    expect(container.getNodeId()).andReturn(new MockNodeId("localhost")).anyTimes();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);

    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();
    CommandScript cs = new CommandScript();
    cs.setScript("scripts/hbase_master.py");
    doReturn(cs).when(mockAps).getScriptPathForMasterPackage(anyString());
    Metainfo metainfo = new Metainfo();
    metainfo.setApplication(new Application());

    Component hm = new Component();
    hm.setName("HBASE_MASTER");
    metainfo.getApplication().addComponent(hm);

    doReturn(metainfo).when(mockAps).getApplicationMetainfo(
        any(SliderFileSystem.class), anyString(), anyBoolean());

    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_ROOT,
        RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);

    try {
      doReturn(true).when(mockAps).isMaster(anyString());
      doNothing().when(mockAps).addInstallCommand(
          eq("HBASE_MASTER"),
          eq("mockcontainer_1"),
          any(HeartBeatResponse.class),
          eq("scripts/hbase_master.py"),
          eq((ComponentCommand)null),
          eq(600L),
          anyString());
      doReturn(conf).when(mockAps).getConfig();
    } catch (SliderException e) {
    }

    doNothing().when(mockAps).processAllocatedPorts(
        anyString(),
        anyString(),
        anyString(),
        anyMap()
    );

    doNothing().when(mockAps).publishFolderPaths(anyMap(),
                                                 anyString(),
                                                 anyString(),
                                                 anyString()
    );
    expect(access.isApplicationLive()).andReturn(true).anyTimes();
    ClusterDescription desc = new ClusterDescription();
    desc.setOption(OptionKeys.ZOOKEEPER_QUORUM, "host1:2181");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getClusterStatus()).andReturn(desc).anyTimes();
    Map<String, Map<String, ClusterNode>> cnMap =
        new HashMap<String, Map<String, ClusterNode>>();
    expect(access.getRoleClusterNodeMapping()).andReturn(cnMap).anyTimes();


    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getOrAddComponent("HBASE_MASTER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    treeOps.set(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getInstanceDefinitionSnapshot()).andReturn(aggConf);
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    replay(access, ctx, container, sliderFileSystem, mockFs);

    try {
      mockAps.buildContainerLaunchContext(launcher,
                                          instanceDefinition,
                                          container,
                                          role,
                                          sliderFileSystem,
                                          generatedConfPath,
                                          resourceComponent,
                                          appComponent,
                                          containerTmpDirPath);
      // JDK7
    } catch (IOException he) {
      log.warn("{}", he, he);
    } catch (SliderException he) {
      log.warn("{}", he, he);
    }

    Register reg = new Register();
    reg.setResponseId(0);
    reg.setLabel("mockcontainer_1___HBASE_MASTER");
    Map<String,String> ports = new HashMap<String, String>();
    ports.put("a","100");
    reg.setAllocatedPorts(ports);
    Map<String, String> folders = new HashMap<String, String>();
    folders.put("F1", "F2");
    reg.setLogFolders(folders);
    RegistrationResponse resp = mockAps.handleRegistration(reg);
    Assert.assertEquals(0, resp.getResponseId());
    Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

    Mockito.verify(mockAps, Mockito.times(1)).processAllocatedPorts(
        anyString(),
        anyString(),
        anyString(),
        anyMap()
    );

    Mockito.verify(mockAps, Mockito.times(1)).publishFolderPaths(
        anyMap(),
        anyString(),
        anyString(),
        anyString()
    );

    HeartBeat hb = new HeartBeat();
    hb.setResponseId(1);
    hb.setHostname("mockcontainer_1___HBASE_MASTER");
    HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
    Assert.assertEquals(2, hbr.getResponseId());
  }


  private AggregateConf prepareConfForAgentStateTests() {
    ConfTree tree = new ConfTree();
    tree.global.put(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");

    AggregateConf instanceDefinition = new AggregateConf();
    instanceDefinition.setInternal(tree);
    instanceDefinition.setAppConf(tree);
    instanceDefinition.getAppConfOperations().getGlobalOptions()
        .put(AgentKeys.APP_DEF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions()
        .put(AgentKeys.AGENT_CONF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions()
        .put(AgentKeys.AGENT_VERSION, ".");
    return instanceDefinition;
  }

  private AgentProviderService prepareProviderServiceForAgentStateTests()
      throws Exception {
    ContainerLaunchContext ctx = createNiceMock(ContainerLaunchContext.class);
    Container container = createNiceMock(Container.class);
    String role = "HBASE_MASTER";
    SliderFileSystem sliderFileSystem = createNiceMock(SliderFileSystem.class);
    FileSystem mockFs = new MockFileSystem();
    expect(sliderFileSystem.getFileSystem()).andReturn(
        new FilterFileSystem(mockFs)).anyTimes();
    expect(
        sliderFileSystem.createAmResource(anyObject(Path.class),
            anyObject(LocalResourceType.class))).andReturn(
        createNiceMock(LocalResource.class)).anyTimes();
    expect(container.getId()).andReturn(new MockContainerId(1)).anyTimes();
    expect(container.getNodeId()).andReturn(new MockNodeId("localhost"))
        .anyTimes();
    expect(container.getPriority()).andReturn(Priority.newInstance(1));

    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);
    Configuration conf = new Configuration();

    AgentProviderService aps = createAgentProviderService(conf);
    AgentProviderService mockAps = Mockito.spy(aps);

    doReturn(access).when(mockAps).getAmState();
    CommandScript cs = new CommandScript();
    cs.setScript("scripts/hbase_master.py");
    doReturn(cs).when(mockAps)
        .getScriptPathForMasterPackage(anyString());
    Metainfo metainfo = new Metainfo();
    Application application = new Application();
    metainfo.setApplication(application);
    doReturn(metainfo).when(mockAps).getApplicationMetainfo(
        any(SliderFileSystem.class), anyString());
    doReturn(metainfo).when(mockAps).getMetaInfo();



    try {
      doReturn(true).when(mockAps).isMaster(anyString());
      doNothing().when(mockAps).addInstallCommand(eq("HBASE_MASTER"),
          eq("mockcontainer_1"), any(HeartBeatResponse.class),
          eq("scripts/hbase_master.py"), eq((ComponentCommand)null),
          eq(600L), anyString());
      doReturn(conf).when(mockAps).getConfig();
    } catch (SliderException e) {
    }

    doNothing().when(mockAps).processAllocatedPorts(anyString(), anyString(),
                                                    anyString(), anyMap());
    expect(access.isApplicationLive()).andReturn(true).anyTimes();
    ClusterDescription desc = new ClusterDescription();
    desc.setOption(OptionKeys.ZOOKEEPER_QUORUM, "host1:2181");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getClusterStatus()).andReturn(desc).anyTimes();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getOrAddComponent("HBASE_MASTER")
        .put(AgentKeys.WAIT_HEARTBEAT, "0");
    treeOps.set(OptionKeys.APPLICATION_NAME, "HBASE");
    treeOps.set("java_home", "/usr/jdk7/");
    treeOps.set("site.fs.defaultFS", "hdfs://c6409.ambari.apache.org:8020");
    treeOps.set(InternalKeys.INTERNAL_DATA_DIR_PATH, "hdfs://c6409.ambari.apache.org:8020/user/yarn/.slider/cluster/cl1/data");
    expect(access.getInstanceDefinitionSnapshot()).andReturn(aggConf);
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.getAppConfSnapshot()).andReturn(treeOps).anyTimes();
    replay(access, ctx, container, sliderFileSystem);

    List<Container> containers = new ArrayList<Container>();
    containers.add(container);
    Map<Integer, ProviderRole> providerRoleMap = new HashMap<Integer, ProviderRole>();
    ProviderRole providerRole = new ProviderRole(role, 1);
    providerRoleMap.put(1, providerRole);
    mockAps.rebuildContainerDetails(containers, "mockcontainer_1",
                                    providerRoleMap);
    return mockAps;
  }

  @Test
  public void testThreeInstallFailures() throws IOException, SliderException {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    ConfTree tree = new ConfTree();
    tree.global.put(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");

    Configuration conf = new Configuration();
    AgentProviderService aps = createAgentProviderService(conf);
    YarnRegistryViewForProviders registryViewForProviders = aps.getYarnRegistry();
    assertNotNull(registryViewForProviders);

    ContainerLaunchContext ctx = createNiceMock(ContainerLaunchContext.class);
    AggregateConf instanceDefinition = new AggregateConf();

    instanceDefinition.setInternal(tree);
    instanceDefinition.setAppConf(tree);
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.APP_DEF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_CONF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_VERSION, ".");

    Container container = createNiceMock(Container.class);
    String role_hm = "HBASE_MASTER";
    SliderFileSystem sliderFileSystem = createNiceMock(SliderFileSystem.class);
    ContainerLauncher launcher = createNiceMock(ContainerLauncher.class);
    Path generatedConfPath = new Path(".", "test");
    MapOperations resourceComponent = new MapOperations();
    MapOperations appComponent = new MapOperations();
    Path containerTmpDirPath = new Path(".", "test");
    FilterFileSystem mockFs = createNiceMock(FilterFileSystem.class);
    expect(sliderFileSystem.getFileSystem())
        .andReturn(mockFs).anyTimes();
    expect(mockFs.exists(anyObject(Path.class))).andReturn(true).anyTimes();
    expect(sliderFileSystem.createAmResource(anyObject(Path.class),
                                             anyObject(LocalResourceType.class)))
        .andReturn(createNiceMock(LocalResource.class)).anyTimes();
    expect(container.getId()).andReturn(new MockContainerId(1)).anyTimes();
    expect(container.getNodeId()).andReturn(new MockNodeId("localhost")).anyTimes();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);

    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();
    doReturn(metainfo).when(mockAps).getApplicationMetainfo(any(SliderFileSystem.class), anyString());
    doReturn(new HashMap<String, DefaultConfig>()).when(mockAps).
        initializeDefaultConfigs(any(SliderFileSystem.class), anyString(), any(Metainfo.class));


    try {
      doReturn(true).when(mockAps).isMaster(anyString());
      doNothing().when(mockAps).addInstallCommand(
          anyString(),
          anyString(),
          any(HeartBeatResponse.class),
          anyString(),
          eq((ComponentCommand)null),
          Mockito.anyLong(),
          anyString());
      doReturn(conf).when(mockAps).getConfig();
    } catch (SliderException e) {
    }

    expect(access.isApplicationLive()).andReturn(true).anyTimes();
    ClusterDescription desc = new ClusterDescription();
    desc.setOption(OptionKeys.ZOOKEEPER_QUORUM, "host1:2181");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getClusterStatus()).andReturn(desc).anyTimes();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getOrAddComponent("HBASE_MASTER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    treeOps.set(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getInstanceDefinitionSnapshot()).andReturn(aggConf).anyTimes();
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    replay(access, ctx, container, sliderFileSystem, mockFs);

    try {
      mockAps.buildContainerLaunchContext(launcher,
                                          instanceDefinition,
                                          container,
                                          role_hm,
                                          sliderFileSystem,
                                          generatedConfPath,
                                          resourceComponent,
                                          appComponent,
                                          containerTmpDirPath);

      Register reg = new Register();
      reg.setResponseId(0);
      reg.setLabel("mockcontainer_1___HBASE_MASTER");
      RegistrationResponse resp = mockAps.handleRegistration(reg);
      Assert.assertEquals(0, resp.getResponseId());
      Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

      HeartBeat hb = new HeartBeat();
      hb.setResponseId(1);
      hb.setHostname("mockcontainer_1___HBASE_MASTER");
      HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(2, hbr.getResponseId());
      Assert.assertFalse(hbr.isTerminateAgent());

      hb.setResponseId(2);
      CommandReport cr = new CommandReport();
      cr.setRole("HBASE_MASTER");
      cr.setRoleCommand("INSTALL");
      cr.setStatus("FAILED");
      hb.setReports(Arrays.asList(cr));
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(3, hbr.getResponseId());
      Assert.assertFalse(hbr.isTerminateAgent());

      hb.setResponseId(3);
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(4, hbr.getResponseId());
      Assert.assertFalse(hbr.isTerminateAgent());

      //Third failure triggers a stop to the agent
      hb.setResponseId(4);
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(5, hbr.getResponseId());
      Assert.assertTrue(hbr.isTerminateAgent());
      Mockito.verify(mockAps, Mockito.times(3)).addInstallCommand(anyString(),
                                                                  anyString(),
                                                                  any(HeartBeatResponse.class),
                                                                  anyString(),
                                                                  eq((ComponentCommand)null),
                                                                  Mockito.anyLong(),
                                                                  null);
    } catch (SliderException he) {
      log.warn(he.getMessage());
    } catch (IOException he) {
      log.warn(he.getMessage());
    }
  }

  @Test
  public void testAgentStateStarted() throws Exception {
    AggregateConf instanceDefinition = prepareConfForAgentStateTests();
    AgentProviderService mockAps = prepareProviderServiceForAgentStateTests();

    Metainfo metainfo = new Metainfo();
    Application application = new Application();
    Component hbaseMaster = new Component();
    hbaseMaster.setName("HBASE_MASTER");
    application.addComponent(hbaseMaster);
    metainfo.setApplication(application);
    doReturn(metainfo).when(mockAps).getMetaInfo();
    doReturn(metainfo).when(mockAps).getApplicationMetainfo(
        any(SliderFileSystem.class), anyString(), anyBoolean());

    Register reg = new Register();
    reg.setResponseId(0);
    reg.setLabel("mockcontainer_1___HBASE_MASTER");
    Map<String,String> ports = new HashMap<String,String>();
    ports.put("a", "100");
    reg.setAllocatedPorts(ports);

    // Simulating agent in STARTED state
    reg.setActualState(State.STARTED);

    mockAps.initializeApplicationConfiguration(instanceDefinition,
        null);

    RegistrationResponse resp = mockAps.handleRegistration(reg);
    Assert.assertEquals(0, resp.getResponseId());
    Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

    Mockito.verify(mockAps, Mockito.times(1)).processAllocatedPorts(
        anyString(),
        anyString(),
        anyString(),
        anyMap()
    );

    HeartBeat hb = new HeartBeat();
    hb.setResponseId(1);
    hb.setHostname("mockcontainer_1___HBASE_MASTER");
    HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
    Assert.assertEquals(2, hbr.getResponseId());
    Assert.assertNotNull(
        "Status command from AM cannot be null when agent's actualState "
            + "is set to STARTED during registration", hbr.getStatusCommands());
    Assert.assertTrue(
        "Status command from AM cannot be empty when agent's actualState "
            + "is set to STARTED during registration", hbr.getStatusCommands()
            .size() > 0);
    Assert.assertEquals(
        "AM should directly send a STATUS request if agent's actualState is "
            + "set to STARTED during registration",
        AgentCommandType.STATUS_COMMAND, hbr.getStatusCommands().get(0)
            .getCommandType());
    Assert.assertEquals(
        "AM should directly request for CONFIG if agent's actualState is "
            + "set to STARTED during registration",
        "GET_CONFIG", hbr.getStatusCommands().get(0)
            .getRoleCommand());
    Assert.assertFalse("AM cannot ask agent to restart", hbr.isRestartAgent());
  }

  @Test
  public void testAgentStateInstalled() throws Exception, SliderException {
    AggregateConf instanceDefinition = prepareConfForAgentStateTests();
    AgentProviderService mockAps = prepareProviderServiceForAgentStateTests();

    Metainfo metainfo = new Metainfo();
    Application application = new Application();
    CommandOrder cmdOrder = new CommandOrder();
    cmdOrder.setCommand("HBASE_MASTER-START");
    cmdOrder.setRequires("HBASE_MASTER-INSTALLED");
    application.getCommandOrders().add(cmdOrder);
    Component hbaseMaster = new Component();
    hbaseMaster.setName("HBASE_MASTER");
    application.addComponent(hbaseMaster);
    metainfo.setApplication(application);
    doReturn(metainfo).when(mockAps).getApplicationMetainfo(
        any(SliderFileSystem.class), anyString(), anyBoolean());
    doReturn(metainfo).when(mockAps).getMetaInfo();
    doNothing().when(mockAps).addRoleRelatedTokens(anyMap());

    Register reg = new Register();
    reg.setResponseId(0);
    reg.setLabel("mockcontainer_1___HBASE_MASTER");
    Map<String,String> ports = new HashMap<String,String>();
    ports.put("a","100");
    reg.setAllocatedPorts(ports);

    // Simulating agent in INSTALLED state
    reg.setActualState(State.INSTALLED);

    mockAps.initializeApplicationConfiguration(instanceDefinition, null);

    RegistrationResponse resp = mockAps.handleRegistration(reg);
    Assert.assertEquals(0, resp.getResponseId());
    Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

    Mockito.verify(mockAps, Mockito.times(1)).processAllocatedPorts(
        anyString(),
        anyString(),
        anyString(),
        anyMap()
    );

    HeartBeat hb = new HeartBeat();
    hb.setResponseId(1);
    hb.setHostname("mockcontainer_1___HBASE_MASTER");
    HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
    Assert.assertEquals(2, hbr.getResponseId());
    Assert.assertNotNull(
        "Execution command from AM cannot be null when agent's actualState "
            + "is set to INSTALLED during registration", hbr.getExecutionCommands());
    Assert.assertTrue(
        "Execution command from AM cannot be empty when agent's actualState "
            + "is set to INSTALLED during registration", hbr.getExecutionCommands()
            .size() > 0);
    Assert.assertEquals(
        "AM should send an EXECUTION command if agent's actualState is "
            + "set to INSTALLED during registration",
        AgentCommandType.EXECUTION_COMMAND, hbr.getExecutionCommands().get(0)
            .getCommandType());
    Assert.assertEquals(
        "AM should request for START if agent's actualState is "
            + "set to INSTALLED during registration",
        "START", hbr.getExecutionCommands().get(0)
            .getRoleCommand());
    Assert.assertFalse("AM cannot ask agent to restart", hbr.isRestartAgent());
  }

  @Test
  public void testRoleHostMapping() throws Exception {
    AgentProviderService aps = createAgentProviderService(new Configuration());
    StateAccessForProviders appState = new ProviderAppState("undefined", null) {
      @Override
      public ClusterDescription getClusterStatus() {
        ClusterDescription cd = new ClusterDescription();
        cd.status = new HashMap<String, Object>();

        cd.status.put(ClusterDescriptionKeys.KEY_CLUSTER_LIVE, buildRoleMap());

        return cd;
      }

      @Override
      public Map<String, Map<String, ClusterNode>> getRoleClusterNodeMapping() {
        return buildRoleMap();
      }

      public Map<String, Map<String, ClusterNode>> buildRoleMap() {
        Map<String, Map<String, ClusterNode>> roleMap = new HashMap<String, Map<String, ClusterNode>>();
        ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
        cn1.host = "FIRST_HOST";
        Map<String, ClusterNode> map1 = new HashMap<String, ClusterNode>();
        map1.put("FIRST_CONTAINER", cn1);
        ClusterNode cn2 = new ClusterNode(new MockContainerId(2));
        cn2.host = "SECOND_HOST";
        Map<String, ClusterNode> map2 = new HashMap<String, ClusterNode>();
        map2.put("SECOND_CONTAINER", cn2);
        ClusterNode cn3 = new ClusterNode(new MockContainerId(3));
        cn3.host = "THIRD_HOST";
        map2.put("THIRD_CONTAINER", cn3);

        roleMap.put("FIRST_ROLE", map1);
        roleMap.put("SECOND_ROLE", map2);
        return roleMap;
      }

      @Override
      public boolean isApplicationLive() {
        return true;
      }

      @Override
      public ClusterDescription refreshClusterStatus() {
        // do nothing
        return null;
      }
    };

    aps.setAmState(appState);
    Map<String, String> tokens = new HashMap<String, String>();
    aps.addRoleRelatedTokens(tokens);
    Assert.assertEquals(2, tokens.size());
    Assert.assertEquals("FIRST_HOST", tokens.get("${FIRST_ROLE_HOST}"));
    Assert.assertEquals("THIRD_HOST,SECOND_HOST", tokens.get("${SECOND_ROLE_HOST}"));
    aps.close();
  }

  @Test
  public void testComponentSpecificPublishes() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    AgentProviderService aps = createAgentProviderService(new Configuration());
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());
    doReturn(metainfo).when(mockAps).getMetaInfo();

    Map<String, String> ports = new HashMap<String, String>();
    ports.put("global.listen_port", "10010");
    mockAps.processAndPublishComponentSpecificData(ports,
                                                   "cid1",
                                                   "host1",
                                                   "HBASE_REGIONSERVER");
    ArgumentCaptor<Collection> entriesCaptor = ArgumentCaptor.
        forClass(Collection.class);
    ArgumentCaptor<String> publishNameCaptor = ArgumentCaptor.
        forClass(String.class);
    Mockito.verify(mockAps, Mockito.times(1)).publishApplicationInstanceData(
        anyString(),
        publishNameCaptor.capture(),
        entriesCaptor.capture());
    assert entriesCaptor.getAllValues().size() == 1;
    for (Collection coll : entriesCaptor.getAllValues()) {
      Set<Map.Entry<String, String>> entrySet = (Set<Map.Entry<String, String>>) coll;
      for (Map.Entry entry : entrySet) {
        log.info("{}:{}", entry.getKey(), entry.getValue().toString());
        if (entry.getKey().equals("PropertyA")) {
          assert entry.getValue().toString().equals("host1:10010");
        }
      }
    }
    assert publishNameCaptor.getAllValues().size() == 1;
    for (String coll : publishNameCaptor.getAllValues()) {
      assert coll.equals("ComponentInstanceData");
    }
  }

  @Test
  public void testComponentSpecificPublishes2() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    AgentProviderService aps = createAgentProviderService(new Configuration());
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());
    doReturn(metainfo).when(mockAps).getMetaInfo();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);
    doReturn(access).when(mockAps).getAmState();
    PublishedExportsSet pubExpSet = new PublishedExportsSet();
    expect(access.getPublishedExportsSet()).andReturn(pubExpSet).anyTimes();
    replay(access);

    Map<String, String> ports = new HashMap<String, String>();
    ports.put("global.listen_port", "10010");
    mockAps.processAndPublishComponentSpecificExports(ports,
                                                      "mockcontainer_1",
                                                      "host1",
                                                      "HBASE_REGIONSERVER");
    ArgumentCaptor<Collection> entriesCaptor = ArgumentCaptor.
        forClass(Collection.class);
    ArgumentCaptor<String> publishNameCaptor = ArgumentCaptor.
        forClass(String.class);
    Mockito.verify(mockAps, Mockito.times(1)).publishApplicationInstanceData(
        anyString(),
        publishNameCaptor.capture(),
        entriesCaptor.capture());

    PublishedExports pubExports = pubExpSet.get("QuickLinks".toLowerCase());
    Assert.assertEquals(1, pubExports.entries.size());
    Assert.assertEquals("QuickLinks", pubExports.description);
    List<ExportEntry> expEntries = pubExports.entries.get("Comp_Endpoint");
    Assert.assertEquals(1, expEntries.size());
    Assert.assertEquals("mockcontainer_1", expEntries.get(0).getContainerId());
    Assert.assertEquals("component", expEntries.get(0).getLevel());
    Assert.assertEquals("1", expEntries.get(0).getTag());
    Assert.assertEquals("http://host1:10010", expEntries.get(0).getValue());
    Assert.assertNotNull(expEntries.get(0).getUpdatedTime());
    Assert.assertNull(expEntries.get(0).getValidUntil());

    assert entriesCaptor.getAllValues().size() == 1;
    for (Collection coll : entriesCaptor.getAllValues()) {
      Set<Map.Entry<String, String>> entrySet = (Set<Map.Entry<String, String>>) coll;
      for (Map.Entry entry : entrySet) {
        log.info("{}:{}", entry.getKey(), entry.getValue().toString());
        if (entry.getKey().equals("Comp_Endpoint")) {
          assert entry.getValue().toString().equals("http://host1:10010");
        }
      }
    }
    assert publishNameCaptor.getAllValues().size() == 1;
    for (String coll : publishNameCaptor.getAllValues()) {
      assert coll.equals("QuickLinks");
    }

    mockAps.notifyContainerCompleted(new MockContainerId(1));
    pubExports = pubExpSet.get("QuickLinks".toLowerCase());
    Assert.assertEquals(1, pubExports.entries.size());
    Assert.assertEquals("QuickLinks", pubExports.description);
    expEntries = pubExports.entries.get("Comp_Endpoint");
    Assert.assertEquals(0, expEntries.size());

    mockAps.notifyContainerCompleted(new MockContainerId(1));
    mockAps.notifyContainerCompleted(new MockContainerId(2));

    mockAps.processAndPublishComponentSpecificExports(ports,
                                                      "mockcontainer_1",
                                                      "host1",
                                                      "HBASE_REGIONSERVER");
    mockAps.processAndPublishComponentSpecificExports(ports,
                                                      "mockcontainer_2",
                                                      "host1",
                                                      "HBASE_REGIONSERVER");
    pubExports = pubExpSet.get("QuickLinks".toLowerCase());
    Assert.assertEquals(1, pubExports.entries.size());
    Assert.assertEquals("QuickLinks", pubExports.description);
    expEntries = pubExports.entries.get("Comp_Endpoint");
    Assert.assertEquals(2, expEntries.size());

    mockAps.notifyContainerCompleted(new MockContainerId(2));
    expEntries = pubExports.entries.get("Comp_Endpoint");
    Assert.assertEquals(1, expEntries.size());
  }

  @Test
  public void testProcessConfig() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    Assert.assertNotNull(metainfo.getApplication());
    AgentProviderService aps = createAgentProviderService(new Configuration());
    HeartBeat hb = new HeartBeat();
    ComponentStatus status = new ComponentStatus();
    status.setClusterName("test");
    status.setComponentName("HBASE_MASTER");
    status.setRoleCommand("GET_CONFIG");
    Map<String, String> hbaseSite = new HashMap<String, String>();
    hbaseSite.put("hbase.master.info.port", "60012");
    hbaseSite.put("c", "d");
    Map<String, Map<String, String>> configs = 
        new HashMap<String, Map<String, String>>();
    configs.put("hbase-site", hbaseSite);
    configs.put("global", hbaseSite);
    status.setConfigs(configs);
    hb.setComponentStatus(new ArrayList<ComponentStatus>(Arrays.asList(status)));

    Map<String, Map<String, ClusterNode>> roleClusterNodeMap = new HashMap<String, Map<String, ClusterNode>>();
    Map<String, ClusterNode> container = new HashMap<String, ClusterNode>();
    ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
    cn1.host = "HOST1";
    container.put("cid1", cn1);
    roleClusterNodeMap.put("HBASE_MASTER", container);

    ComponentInstanceState componentStatus = new ComponentInstanceState("HBASE_MASTER", 
        new MockContainerId(1), "cid");
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());
    doReturn(metainfo).when(mockAps).getMetaInfo();
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);
    doReturn(access).when(mockAps).getAmState();
    PublishedExportsSet pubExpSet = new PublishedExportsSet();
    expect(access.getPublishedExportsSet()).andReturn(pubExpSet).anyTimes();
    replay(access);

    mockAps.publishConfigAndExportGroups(hb, componentStatus, "HBASE_MASTER");
    Assert.assertTrue(componentStatus.getConfigReported());
    ArgumentCaptor<Collection> entriesCaptor = ArgumentCaptor.
        forClass(Collection.class);
    Mockito.verify(mockAps, Mockito.times(3)).publishApplicationInstanceData(
        anyString(),
        anyString(),
        entriesCaptor.capture());
    Assert.assertEquals(3, entriesCaptor.getAllValues().size());
    for (Collection coll : entriesCaptor.getAllValues()) {
      Set<Map.Entry<String, String>> entrySet = (Set<Map.Entry<String, String>>) coll;
      for (Map.Entry entry : entrySet) {
        log.info("{}:{}", entry.getKey(), entry.getValue().toString());
        if (entry.getKey().equals("JMX_Endpoint")) {
          assert entry.getValue().toString().equals("http://HOST1:60012/jmx");
        }
      }
    }

    Map<String, List<ExportEntry>> exports = mockAps.getCurrentExports("QuickLinks");
    Assert.assertEquals(2, exports.size());
    Assert.assertEquals(exports.get("JMX_Endpoint").get(0).getValue(), "http://HOST1:60012/jmx");

    mockAps.publishConfigAndExportGroups(hb, componentStatus, "HBASE_REST");
    Mockito.verify(mockAps, Mockito.times(3)).publishApplicationInstanceData(
        anyString(),
        anyString(),
        entriesCaptor.capture());
    PublishedExports pubExports = pubExpSet.get("QuickLinks".toLowerCase());
    Assert.assertEquals(2, pubExports.entries.size());
    Assert.assertEquals("QuickLinks", pubExports.description);
    List<ExportEntry> expEntries = pubExports.entries.get("JMX_Endpoint");
    Assert.assertEquals(1, expEntries.size());
    Assert.assertEquals(null, expEntries.get(0).getContainerId());
    Assert.assertEquals("application", expEntries.get(0).getLevel());
    Assert.assertEquals(null, expEntries.get(0).getTag());
    Assert.assertEquals("http://HOST1:60012/jmx", expEntries.get(0).getValue());
    Assert.assertNull(expEntries.get(0).getValidUntil());

    expEntries = pubExports.entries.get("Master_Status");
    Assert.assertEquals(1, expEntries.size());
    expEntries = pubExports.entries.get("JMX_Endpoint");
    Assert.assertEquals("http://HOST1:60012/jmx", expEntries.get(0).getValue());
  }

  @Test
  public void testMetaInfoParsing() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    Assert.assertNotNull(metainfo.getApplication());
    Application application = metainfo.getApplication();
    log.info("Service: " + application.toString());
    Assert.assertEquals(application.getName(), "HBASE");
    Assert.assertEquals(application.getExportedConfigs(), "hbase-site,global");
    Assert.assertEquals(application.getComponents().size(), 3);
    List<Component> components = application.getComponents();
    int found = 0;
    for (Component component : components) {
      if (component.getName().equals("HBASE_MASTER")) {
        Assert.assertEquals("true", component.getAutoStartOnFailure());
        Assert.assertEquals(Boolean.TRUE, component.getAutoStartOnFailureBoolean());
        Assert.assertEquals(component.getMinInstanceCountInt(), 1);
        Assert.assertEquals(component.getMaxInstanceCountInt(), 2);
        Assert.assertEquals(component.getCommandScript().getScript(), "scripts/hbase_master.py");
        Assert.assertEquals(component.getCategory(), "MASTER");
        Assert.assertEquals(component.getAppExports(), "QuickLinks-JMX_Endpoint,QuickLinks-Master_Status");
        Assert.assertEquals(component.getCompExports(), "QuickLinks-Comp_Endpoint");
        found++;
      }
      if (component.getName().equals("HBASE_REGIONSERVER")) {
        Assert.assertEquals("Falsee", component.getAutoStartOnFailure());
        Assert.assertEquals(Boolean.FALSE, component.getAutoStartOnFailureBoolean());
        Assert.assertEquals(component.getMinInstanceCount(), "1");
        Assert.assertNull(component.getMaxInstanceCount());
        Assert.assertEquals(component.getCommandScript().getScript(), "scripts/hbase_regionserver.py");
        Assert.assertEquals(component.getCategory(), "SLAVE");
        found++;
      }
    }
    Assert.assertEquals(found, 2);

    Assert.assertEquals(application.getExportGroups().size(), 1);
    List<ExportGroup> egs = application.getExportGroups();
    ExportGroup eg = egs.get(0);
    Assert.assertEquals(eg.getName(), "QuickLinks");
    Assert.assertEquals(eg.getExports().size(), 3);

    found = 0;
    for (Export export : eg.getExports()) {
      if (export.getName().equals("JMX_Endpoint")) {
        found++;
        Assert.assertEquals(export.getValue(),
                            "http://${HBASE_MASTER_HOST}:${site.hbase-site.hbase.master.info.port}/jmx");
      }
      if (export.getName().equals("Master_Status")) {
        found++;
        Assert.assertEquals(export.getValue(),
                            "http://${HBASE_MASTER_HOST}:${site.hbase-site.hbase.master.info.port}/master-status");
      }
    }
    Assert.assertEquals(found, 2);

    List<CommandOrder> cmdOrders = application.getCommandOrders();
    Assert.assertEquals(cmdOrders.size(), 2);
    found = 0;
    for (CommandOrder co : application.getCommandOrders()) {
      if (co.getCommand().equals("HBASE_REGIONSERVER-START")) {
        Assert.assertTrue(co.getRequires().equals("HBASE_MASTER-STARTED"));
        found++;
      }
      if (co.getCommand().equals("A-START")) {
        Assert.assertEquals(co.getRequires(), "B-STARTED");
        found++;
      }
    }
    Assert.assertEquals(found, 2);

    List<ConfigFile> configFiles = application.getConfigFiles();
    Assert.assertEquals(configFiles.size(), 2);
    found = 0;
    for (ConfigFile configFile : configFiles) {
      if (configFile.getDictionaryName().equals("hbase-site")) {
        Assert.assertEquals("hbase-site.xml", configFile.getFileName());
        Assert.assertEquals("xml", configFile.getType());
        found++;
      }
      if (configFile.getDictionaryName().equals("hbase-env")) {
        Assert.assertEquals("hbase-env.sh", configFile.getFileName());
        Assert.assertEquals("env", configFile.getType());
        found++;
      }
    }
    Assert.assertEquals("Two config dependencies must be found.", found, 2);

    AgentProviderService aps = createAgentProviderService(new Configuration());
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(metainfo).when(mockAps).getMetaInfo();
    CommandScript script = mockAps.getScriptPathForMasterPackage("HBASE_MASTER");
    Assert.assertEquals(script.getScript(), "scripts/hbase_master.py");

    String metainfo_1_str_bad = "<metainfo>\n"
                                + "  <schemaVersion>2.0</schemaVersion>\n"
                                + "  <services>\n"
                                + "    <service>\n"
                                + "      <name>HBASE</name>\n"
                                + "      <comment>\n"
                                + "        Apache HBase\n"
                                + "      </comment>\n";

    metainfo_1 = new ByteArrayInputStream(metainfo_1_str_bad.getBytes());
    metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    Assert.assertNull(metainfo);
  }

  @Test
  public void testMetaInfoRelatedOperations() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    InputStream metainfo_2 = new ByteArrayInputStream(metainfo_2_str.getBytes());
    Metainfo metainfo2 = new MetainfoParser().fromXmlStream(metainfo_2);
    String role_hm = "HBASE_MASTER";
    String role_hrs = "HBASE_REGIONSERVER";

    AgentProviderService aps1 = createAgentProviderService(new Configuration());
    AgentProviderService mockAps = Mockito.spy(aps1);
    doReturn(metainfo).when(mockAps).getMetaInfo();

    AgentProviderService mockAps2 = Mockito.spy(aps1);
    doReturn(metainfo2).when(mockAps2).getMetaInfo();

    Assert.assertTrue(mockAps.isMaster(role_hm));
    Assert.assertFalse(mockAps.isMaster(role_hrs));
    Assert.assertTrue(mockAps.canPublishConfig(role_hm));
    Assert.assertFalse(mockAps.canPublishConfig(role_hrs));
    Assert.assertTrue(mockAps.canAnyMasterPublishConfig());

    Assert.assertTrue(mockAps2.isMaster(role_hm));
    Assert.assertFalse(mockAps2.isMaster(role_hrs));
    Assert.assertTrue(mockAps2.canPublishConfig(role_hm));
    Assert.assertFalse(mockAps2.canPublishConfig(role_hrs));
    Assert.assertTrue(mockAps2.canAnyMasterPublishConfig());
  }

  @Test
  public void testOrchestratedAppStart() throws Exception {
    // App has two components HBASE_MASTER and HBASE_REGIONSERVER
    // Start of HBASE_RS depends on the start of HBASE_MASTER
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    ConfTree tree = new ConfTree();
    tree.global.put(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");

    Configuration conf = new Configuration();
    AgentProviderService aps = createAgentProviderService(conf);
    YarnRegistryViewForProviders registryViewForProviders = aps.getYarnRegistry();
    assertNotNull(registryViewForProviders);
    
    ContainerLaunchContext ctx = createNiceMock(ContainerLaunchContext.class);
    AggregateConf instanceDefinition = new AggregateConf();

    instanceDefinition.setInternal(tree);
    instanceDefinition.setAppConf(tree);
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.APP_DEF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_CONF, ".");
    instanceDefinition.getAppConfOperations().getGlobalOptions().put(AgentKeys.AGENT_VERSION, ".");

    Container container = createNiceMock(Container.class);
    String role_hm = "HBASE_MASTER";
    String role_hrs = "HBASE_REGIONSERVER";
    SliderFileSystem sliderFileSystem = createNiceMock(SliderFileSystem.class);
    ContainerLauncher launcher = createNiceMock(ContainerLauncher.class);
    ContainerLauncher launcher2 = createNiceMock(ContainerLauncher.class);
    Path generatedConfPath = new Path(".", "test");
    MapOperations resourceComponent = new MapOperations();
    MapOperations appComponent = new MapOperations();
    Path containerTmpDirPath = new Path(".", "test");
    FilterFileSystem mockFs = createNiceMock(FilterFileSystem.class);
    expect(sliderFileSystem.getFileSystem())
        .andReturn(mockFs).anyTimes();
    expect(mockFs.exists(anyObject(Path.class))).andReturn(true).anyTimes();
    expect(sliderFileSystem.createAmResource(anyObject(Path.class),
                                             anyObject(LocalResourceType.class)))
        .andReturn(createNiceMock(LocalResource.class)).anyTimes();
    expect(container.getId()).andReturn(new MockContainerId(1)).anyTimes();
    expect(container.getNodeId()).andReturn(new MockNodeId("localhost")).anyTimes();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);

    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();
    doReturn(metainfo).when(mockAps).getApplicationMetainfo(
        any(SliderFileSystem.class), anyString(), anyBoolean());
    doReturn(new HashMap<String, DefaultConfig>()).when(mockAps).
        initializeDefaultConfigs(any(SliderFileSystem.class), anyString(), any(Metainfo.class));


    try {
      doReturn(true).when(mockAps).isMaster(anyString());
      doNothing().when(mockAps).addInstallCommand(
          anyString(),
          anyString(),
          any(HeartBeatResponse.class),
          anyString(),
          eq((ComponentCommand)null),
          Mockito.anyLong(),
          anyString());
      doNothing().when(mockAps).addStartCommand(
          anyString(),
          anyString(),
          any(HeartBeatResponse.class),
          anyString(),
          eq((ComponentCommand)null),
          eq((ComponentCommand)null),
          Mockito.anyLong(),
          Matchers.anyBoolean());
      doNothing().when(mockAps).addGetConfigCommand(
          anyString(),
          anyString(),
          any(HeartBeatResponse.class));
      doNothing().when(mockAps).publishFolderPaths(
          anyMap(),
          anyString(),
          anyString(),
          anyString());
      doReturn(conf).when(mockAps).getConfig();
    } catch (SliderException e) {
    }

    PublishedExportsSet pubExpSet = new PublishedExportsSet();
    expect(access.isApplicationLive()).andReturn(true).anyTimes();
    expect(access.getPublishedExportsSet()).andReturn(pubExpSet).anyTimes();
    ClusterDescription desc = new ClusterDescription();
    desc.setOption(OptionKeys.ZOOKEEPER_QUORUM, "host1:2181");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getClusterStatus()).andReturn(desc).anyTimes();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getOrAddComponent("HBASE_MASTER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    treeOps.getOrAddComponent("HBASE_REGIONSERVER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    treeOps.set(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getInstanceDefinitionSnapshot()).andReturn(aggConf).anyTimes();
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());
    replay(access, ctx, container, sliderFileSystem, mockFs);

    // build two containers
    try {
      mockAps.buildContainerLaunchContext(launcher,
                                          instanceDefinition,
                                          container,
                                          role_hm,
                                          sliderFileSystem,
                                          generatedConfPath,
                                          resourceComponent,
                                          appComponent,
                                          containerTmpDirPath);

      mockAps.buildContainerLaunchContext(launcher2,
                                          instanceDefinition,
                                          container,
                                          role_hrs,
                                          sliderFileSystem,
                                          generatedConfPath,
                                          resourceComponent,
                                          appComponent,
                                          containerTmpDirPath);

      // Both containers register
      Register reg = new Register();
      reg.setResponseId(0);
      reg.setLabel("mockcontainer_1___HBASE_MASTER");
      RegistrationResponse resp = mockAps.handleRegistration(reg);
      Assert.assertEquals(0, resp.getResponseId());
      Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

      reg = new Register();
      reg.setResponseId(0);
      reg.setLabel("mockcontainer_1___HBASE_REGIONSERVER");
      resp = mockAps.handleRegistration(reg);
      Assert.assertEquals(0, resp.getResponseId());
      Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

      // Both issue install command
      HeartBeat hb = new HeartBeat();
      hb.setResponseId(1);
      hb.setHostname("mockcontainer_1___HBASE_MASTER");
      HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(2, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(1)).addInstallCommand(anyString(),
                                                                  anyString(),
                                                                  any(HeartBeatResponse.class),
                                                                  anyString(),
                                                                  eq((ComponentCommand)null),
                                                                  Mockito.anyLong(),
                                                                  anyString());

      hb = new HeartBeat();
      hb.setResponseId(1);
      hb.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(2, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(2)).addInstallCommand(anyString(),
                                                                  anyString(),
                                                                  any(HeartBeatResponse.class),
                                                                  anyString(),
                                                                  eq((ComponentCommand)null),
                                                                  Mockito.anyLong(),
                                                                  anyString());
      // RS succeeds install but does not start
      hb = new HeartBeat();
      hb.setResponseId(2);
      hb.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
      CommandReport cr = new CommandReport();
      cr.setRole("HBASE_REGIONSERVER");
      cr.setRoleCommand("INSTALL");
      cr.setStatus("COMPLETED");
      cr.setFolders(new HashMap<String, String>() {{
        put("a", "b");
      }});
      hb.setReports(Arrays.asList(cr));
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(3, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(0)).addStartCommand(anyString(),
                                                                anyString(),
                                                                any(HeartBeatResponse.class),
                                                                anyString(),
                                                                eq((ComponentCommand)null),
                                                                eq((ComponentCommand)null),
                                                                Mockito.anyLong(),
                                                                Matchers.anyBoolean());
      // RS still does not start
      hb = new HeartBeat();
      hb.setResponseId(3);
      hb.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(4, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(0)).addStartCommand(anyString(),
                                                                anyString(),
                                                                any(HeartBeatResponse.class),
                                                                anyString(),
                                                                eq((ComponentCommand)null),
                                                                eq((ComponentCommand)null),
                                                                Mockito.anyLong(),
                                                                Matchers.anyBoolean());

      // MASTER succeeds install and issues start
      hb = new HeartBeat();
      hb.setResponseId(2);
      hb.setHostname("mockcontainer_1___HBASE_MASTER");
      hb.setFqdn("host1");
      cr = new CommandReport();
      cr.setRole("HBASE_MASTER");
      cr.setRoleCommand("INSTALL");
      cr.setStatus("COMPLETED");
      Map<String, String> ap = new HashMap<String, String>();
      ap.put("a.port", "10233");
      cr.setAllocatedPorts(ap);
      hb.setReports(Arrays.asList(cr));
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(3, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(1)).addStartCommand(anyString(),
                                                                anyString(),
                                                                any(HeartBeatResponse.class),
                                                                anyString(),
                                                                eq((ComponentCommand)null),
                                                                eq((ComponentCommand)null),
                                                                Mockito.anyLong(),
                                                                Matchers.anyBoolean());
      Map<String, String> allocatedPorts = mockAps.getAllocatedPorts();
      Assert.assertTrue(allocatedPorts != null);
      Assert.assertTrue(allocatedPorts.size() == 1);
      Assert.assertTrue(allocatedPorts.containsKey("a.port"));

      // RS still does not start
      hb = new HeartBeat();
      hb.setResponseId(4);
      hb.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(5, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(1)).addStartCommand(anyString(),
                                                                anyString(),
                                                                any(HeartBeatResponse.class),
                                                                anyString(),
                                                                eq((ComponentCommand)null),
                                                                eq((ComponentCommand)null),
                                                                Mockito.anyLong(),
                                                                Matchers.anyBoolean());
      // MASTER succeeds start
      hb = new HeartBeat();
      hb.setResponseId(3);
      hb.setHostname("mockcontainer_1___HBASE_MASTER");
      cr = new CommandReport();
      cr.setRole("HBASE_MASTER");
      cr.setRoleCommand("START");
      cr.setStatus("COMPLETED");
      hb.setReports(Arrays.asList(cr));
      mockAps.handleHeartBeat(hb);
      Mockito.verify(mockAps, Mockito.times(1)).addGetConfigCommand(anyString(),
                                                                    anyString(),
                                                                    any(HeartBeatResponse.class));

      // RS starts now
      hb = new HeartBeat();
      hb.setResponseId(5);
      hb.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(6, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(2)).addStartCommand(anyString(),
                                                                anyString(),
                                                                any(HeartBeatResponse.class),
                                                                anyString(),
                                                                eq((ComponentCommand)null),
                                                                eq((ComponentCommand)null),
                                                                Mockito.anyLong(),
                                                                Matchers.anyBoolean());
    // JDK7 
    } catch (SliderException he) {
      log.warn(he.getMessage());
    } catch (IOException he) {
      log.warn(he.getMessage());
    }

    Mockito.verify(mockAps, Mockito.times(1)).publishFolderPaths(
        anyMap(),
        anyString(),
        anyString(),
        anyString());
  }

  protected AgentProviderService createAgentProviderService(Configuration conf) throws
      IOException {
    AgentProviderService aps = new AgentProviderService();
    YarnRegistryViewForProviders registryViewForProviders =
        createYarnRegistryViewForProviders(conf);
    aps.bindToYarnRegistry(registryViewForProviders);
    return aps;
  }
  
  protected YarnRegistryViewForProviders createYarnRegistryViewForProviders(
      Configuration conf) throws IOException {
    conf.set(RegistryConstants.KEY_REGISTRY_ZK_ROOT,
        RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);

    RegistryOperations registryOperations = new MockRegistryOperations();
    registryOperations.init(conf);
    YarnRegistryViewForProviders registryViewForProviders =
        new YarnRegistryViewForProviders(registryOperations,
            "hbase",
            SliderKeys.APP_TYPE,
            "hbase1",
            new MockApplicationAttemptId(new MockApplicationId(1), 1));
    registryViewForProviders.registerSelf(new ServiceRecord(), true);
    return registryViewForProviders;
  }

  @Test
  public void testPublishFolderPaths() throws IOException {
    AgentProviderService aps = createAgentProviderService(new Configuration());
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();
    PublishedExportsSet pubExpSet = new PublishedExportsSet();
    expect(access.getPublishedExportsSet()).andReturn(pubExpSet).anyTimes();
    replay(access);

    Map<String, String> folders = new HashMap<String, String>();
    folders.put("AGENT_LOG_ROOT", "aFolder");
    folders.put("AGENT_WORK_ROOT", "folderB");
    mockAps.publishFolderPaths(folders, "cid", "role", "fqdn");

    PublishedExports exports = pubExpSet.get("container_log_dirs");
    Assert.assertEquals(1, exports.entries.size());
    List<ExportEntry> expEntries = exports.entries.get("role");
    Assert.assertEquals(1, expEntries.size());
    Assert.assertEquals("cid", expEntries.get(0).getContainerId());
    Assert.assertEquals("component", expEntries.get(0).getLevel());
    Assert.assertEquals("role", expEntries.get(0).getTag());
    Assert.assertEquals("fqdn:aFolder", expEntries.get(0).getValue());
    Assert.assertNull(expEntries.get(0).getValidUntil());
    Assert.assertEquals(null, expEntries.get(0).getValidUntil());

    exports = pubExpSet.get("container_work_dirs");
    Assert.assertEquals(1, exports.entries.size());
    expEntries = exports.entries.get("role");
    Assert.assertEquals(1, expEntries.size());
    Assert.assertEquals("cid", expEntries.get(0).getContainerId());
    Assert.assertEquals("component", expEntries.get(0).getLevel());
    Assert.assertEquals("role", expEntries.get(0).getTag());
    Assert.assertEquals("fqdn:folderB", expEntries.get(0).getValue());
    Assert.assertNull(expEntries.get(0).getValidUntil());
    Assert.assertEquals(null, expEntries.get(0).getValidUntil());
  }

  @Test
  public void testNotifyContainerCompleted() throws IOException {
    AgentProviderService aps = createAgentProviderService(new Configuration());
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());

    ContainerId cid = new MockContainerId(1);
    String id = cid.toString();
    ContainerId cid2 = new MockContainerId(2);
    mockAps.getAllocatedPorts().put("a", "100");
    mockAps.getAllocatedPorts(id).put("a", "100");
    mockAps.getAllocatedPorts(id).put("b", "101");
    mockAps.getAllocatedPorts("cid2").put("c", "102");

    mockAps.getComponentInstanceData().put("cid2", new HashMap<String, String>());
    mockAps.getComponentInstanceData().put(id, new HashMap<String, String>());

    mockAps.getComponentStatuses().put("cid2_HM", new ComponentInstanceState("HM", cid2, "aid"));
    mockAps.getComponentStatuses().put(id + "_HM", new ComponentInstanceState("HM", cid, "aid"));

    Assert.assertNotNull(mockAps.getComponentInstanceData().get(id));
    Assert.assertNotNull(mockAps.getComponentInstanceData().get("cid2"));

    Assert.assertNotNull(mockAps.getComponentStatuses().get(id + "_HM"));
    Assert.assertNotNull(mockAps.getComponentStatuses().get("cid2_HM"));

    Assert.assertEquals(mockAps.getAllocatedPorts().size(), 1);
    Assert.assertEquals(mockAps.getAllocatedPorts(id).size(), 2);
    Assert.assertEquals(mockAps.getAllocatedPorts("cid2").size(), 1);

    // Make the call
    mockAps.notifyContainerCompleted(new MockContainerId(1));

    Assert.assertEquals(mockAps.getAllocatedPorts().size(), 0);
    Assert.assertEquals(mockAps.getAllocatedPorts(id).size(), 0);
    Assert.assertEquals(mockAps.getAllocatedPorts("cid2").size(), 1);

    Assert.assertNull(mockAps.getComponentInstanceData().get(id));
    Assert.assertNotNull(mockAps.getComponentInstanceData().get("cid2"));

    Assert.assertNull(mockAps.getComponentStatuses().get(id + "_HM"));
    Assert.assertNotNull(mockAps.getComponentStatuses().get("cid2_HM"));
  }

  @Test
  public void testAddInstallCommand() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().fromXmlStream(metainfo_1);
    AgentProviderService aps = createAgentProviderService(new Configuration());
    HeartBeatResponse hbr = new HeartBeatResponse();

    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getGlobalOptions().put(AgentKeys.JAVA_HOME, "java_home");
    treeOps.set(OptionKeys.APPLICATION_NAME, "HBASE");
    treeOps.set("site.fs.defaultFS", "hdfs://HOST1:8020/");
    treeOps.set("internal.data.dir.path", "hdfs://HOST1:8020/database");
    treeOps.set(OptionKeys.ZOOKEEPER_HOSTS, "HOST1");

    expect(access.getAppConfSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.isApplicationLive()).andReturn(true).anyTimes();

    doReturn("HOST1").when(mockAps).getClusterInfoPropertyValue(anyString());
    doReturn(metainfo).when(mockAps).getMetaInfo();
    doReturn(new HashMap<String, DefaultConfig>()).when(mockAps).getDefaultConfigs();

    Map<String, Map<String, ClusterNode>> roleClusterNodeMap = new HashMap<String, Map<String, ClusterNode>>();
    Map<String, ClusterNode> container = new HashMap<String, ClusterNode>();
    ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
    cn1.host = "HOST1";
    container.put("cid1", cn1);
    roleClusterNodeMap.put("HBASE_MASTER", container);
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();

    replay(access);

    mockAps.addInstallCommand("HBASE_MASTER", "cid1", hbr, "", null, 0, null);
    ExecutionCommand cmd = hbr.getExecutionCommands().get(0);
    String pkgs = cmd.getHostLevelParams().get(AgentKeys.PACKAGE_LIST);
    Assert.assertEquals("[{\"type\":\"tarball\",\"name\":\"files/hbase-0.96.1-hadoop2-bin.tar.gz\"}]", pkgs);
    Assert.assertEquals("java_home", cmd.getHostLevelParams().get(AgentKeys.JAVA_HOME));
    Assert.assertEquals("cid1", cmd.getHostLevelParams().get("container_id"));
    Assert.assertEquals(Command.INSTALL.toString(), cmd.getRoleCommand());
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_log_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_pid_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_install_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_input_conf_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_container_id"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("pid_file"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_root"));
  }

  @Test
  public void testAddStartCommand() throws Exception {
    AgentProviderService aps = createAgentProviderService(new Configuration());
    HeartBeatResponse hbr = new HeartBeatResponse();

    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getGlobalOptions().put(AgentKeys.JAVA_HOME, "java_home");
    treeOps.set(OptionKeys.APPLICATION_NAME, "HBASE");
    treeOps.set("site.fs.defaultFS", "hdfs://HOST1:8020/");
    treeOps.set("internal.data.dir.path", "hdfs://HOST1:8020/database");
    treeOps.set(OptionKeys.ZOOKEEPER_HOSTS, "HOST1");
    treeOps.getGlobalOptions().put("site.hbase-site.a.port", "${HBASE_MASTER.ALLOCATED_PORT}");
    treeOps.getGlobalOptions().put("site.hbase-site.b.port", "${HBASE_MASTER.ALLOCATED_PORT}");
    treeOps.getGlobalOptions().put("site.hbase-site.random.port", "${HBASE_MASTER.ALLOCATED_PORT}{PER_CONTAINER}");
    treeOps.getGlobalOptions().put("site.hbase-site.random2.port", "${HBASE_MASTER.ALLOCATED_PORT}");

    Map<String, DefaultConfig> defaultConfigMap = new HashMap<String, DefaultConfig>();
    DefaultConfig defaultConfig = new DefaultConfig();
    PropertyInfo propertyInfo1 = new PropertyInfo();
    propertyInfo1.setName("defaultA");
    propertyInfo1.setValue("Avalue");
    defaultConfig.addPropertyInfo(propertyInfo1);
    propertyInfo1 = new PropertyInfo();
    propertyInfo1.setName("defaultB");
    propertyInfo1.setValue("");
    defaultConfig.addPropertyInfo(propertyInfo1);
    defaultConfigMap.put("hbase-site", defaultConfig);

    expect(access.getAppConfSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.isApplicationLive()).andReturn(true).anyTimes();

    doReturn("HOST1").when(mockAps).getClusterInfoPropertyValue(anyString());
    doReturn(defaultConfigMap).when(mockAps).getDefaultConfigs();
    List<String> configurations = new ArrayList<String>();
    configurations.add("hbase-site");
    configurations.add("global");
    List<String> sysConfigurations = new ArrayList<String>();
    configurations.add("core-site");
    doReturn(configurations).when(mockAps).getApplicationConfigurationTypes();
    doReturn(sysConfigurations).when(mockAps).getSystemConfigurationsRequested(any(ConfTreeOperations.class));

    Map<String, Map<String, ClusterNode>> roleClusterNodeMap = new HashMap<String, Map<String, ClusterNode>>();
    Map<String, ClusterNode> container = new HashMap<String, ClusterNode>();
    ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
    cn1.host = "HOST1";
    container.put("cid1", cn1);
    roleClusterNodeMap.put("HBASE_MASTER", container);
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();
    Map<String, String> allocatedPorts = new HashMap<String, String>();
    allocatedPorts.put("hbase-site.a.port", "10023");
    allocatedPorts.put("hbase-site.b.port", "10024");
    doReturn(allocatedPorts).when(mockAps).getAllocatedPorts();
    Map<String, String> allocatedPorts2 = new HashMap<String, String>();
    allocatedPorts2.put("hbase-site.random.port", "10025");
    doReturn(allocatedPorts2).when(mockAps).getAllocatedPorts(anyString());

    replay(access);

    ComponentCommand startCmd = ComponentCommand.getDefaultComponentCommand();
    ComponentCommand stopCmd = ComponentCommand.getDefaultComponentCommand("STOP");
    mockAps.addStartCommand("HBASE_MASTER", "cid1", hbr, "", startCmd, stopCmd, 0, Boolean.FALSE);
    Assert.assertTrue(hbr.getExecutionCommands().get(0).getConfigurations().containsKey("hbase-site"));
    Assert.assertTrue(hbr.getExecutionCommands().get(0).getConfigurations().containsKey("core-site"));
    Map<String, String> hbaseSiteConf = hbr.getExecutionCommands().get(0).getConfigurations().get("hbase-site");
    Assert.assertTrue(hbaseSiteConf.containsKey("a.port"));
    Assert.assertEquals("10023", hbaseSiteConf.get("a.port"));
    Assert.assertEquals("10024", hbaseSiteConf.get("b.port"));
    Assert.assertEquals("10025", hbaseSiteConf.get("random.port"));
    assertEquals("${HBASE_MASTER.ALLOCATED_PORT}",
                 hbaseSiteConf.get("random2.port"));
    ExecutionCommand cmd = hbr.getExecutionCommands().get(0);
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_log_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_pid_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_install_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_input_conf_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_container_id"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("pid_file"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_root"));
    Assert.assertTrue(cmd.getConfigurations().get("hbase-site").containsKey("defaultA"));
    Assert.assertFalse(cmd.getConfigurations().get("hbase-site").containsKey("defaultB"));
  }

  @Test
  public void testAddStopCommand() throws Exception {
    AgentProviderService aps = createAgentProviderService(new Configuration());
    HeartBeatResponse hbr = new HeartBeatResponse();

    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getGlobalOptions().put(AgentKeys.JAVA_HOME, "java_home");
    treeOps.set(OptionKeys.APPLICATION_NAME, "HBASE");
    treeOps.set("site.fs.defaultFS", "hdfs://HOST1:8020/");
    treeOps.set("internal.data.dir.path", "hdfs://HOST1:8020/database");
    treeOps.set(OptionKeys.ZOOKEEPER_HOSTS, "HOST1");
    treeOps.getGlobalOptions().put("site.hbase-site.a.port", "${HBASE_MASTER.ALLOCATED_PORT}");
    treeOps.getGlobalOptions().put("site.hbase-site.b.port", "${HBASE_MASTER.ALLOCATED_PORT}");
    treeOps.getGlobalOptions().put("site.hbase-site.random.port", "${HBASE_MASTER.ALLOCATED_PORT}{PER_CONTAINER}");
    treeOps.getGlobalOptions().put("site.hbase-site.random2.port", "${HBASE_MASTER.ALLOCATED_PORT}");

    Map<String, DefaultConfig> defaultConfigMap = new HashMap<String, DefaultConfig>();
    DefaultConfig defaultConfig = new DefaultConfig();
    PropertyInfo propertyInfo1 = new PropertyInfo();
    propertyInfo1.setName("defaultA");
    propertyInfo1.setValue("Avalue");
    defaultConfig.addPropertyInfo(propertyInfo1);
    propertyInfo1 = new PropertyInfo();
    propertyInfo1.setName("defaultB");
    propertyInfo1.setValue("");
    defaultConfig.addPropertyInfo(propertyInfo1);
    defaultConfigMap.put("hbase-site", defaultConfig);

    expect(access.getAppConfSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.isApplicationLive()).andReturn(true).anyTimes();

    doReturn("HOST1").when(mockAps).getClusterInfoPropertyValue(anyString());
    doReturn(defaultConfigMap).when(mockAps).getDefaultConfigs();
    List<String> configurations = new ArrayList<String>();
    configurations.add("hbase-site");
    configurations.add("global");
    List<String> sysConfigurations = new ArrayList<String>();
    configurations.add("core-site");
    doReturn(configurations).when(mockAps).getApplicationConfigurationTypes();
    doReturn(sysConfigurations).when(mockAps).getSystemConfigurationsRequested(any(ConfTreeOperations.class));

    Map<String, Map<String, ClusterNode>> roleClusterNodeMap = new HashMap<String, Map<String, ClusterNode>>();
    Map<String, ClusterNode> container = new HashMap<String, ClusterNode>();
    ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
    cn1.host = "HOST1";
    container.put("cid1", cn1);
    roleClusterNodeMap.put("HBASE_MASTER", container);
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();
    Map<String, String> allocatedPorts = new HashMap<String, String>();
    allocatedPorts.put("hbase-site.a.port", "10023");
    allocatedPorts.put("hbase-site.b.port", "10024");
    doReturn(allocatedPorts).when(mockAps).getAllocatedPorts();
    Map<String, String> allocatedPorts2 = new HashMap<String, String>();
    allocatedPorts2.put("hbase-site.random.port", "10025");
    doReturn(allocatedPorts2).when(mockAps).getAllocatedPorts(anyString());

    replay(access);

    mockAps.addStopCommand("HBASE_MASTER", "cid1", hbr, "/tmp/stop_cmd.sh", 10, false);

    Assert.assertTrue(hbr.getExecutionCommands().get(0).getConfigurations().containsKey("hbase-site"));
    Assert.assertTrue(hbr.getExecutionCommands().get(0).getConfigurations().containsKey("core-site"));
    Map<String, String> hbaseSiteConf = hbr.getExecutionCommands().get(0).getConfigurations().get("hbase-site");
    Assert.assertTrue(hbaseSiteConf.containsKey("a.port"));
    Assert.assertEquals("10023", hbaseSiteConf.get("a.port"));
    Assert.assertEquals("10024", hbaseSiteConf.get("b.port"));
    Assert.assertEquals("10025", hbaseSiteConf.get("random.port"));
    assertEquals("${HBASE_MASTER.ALLOCATED_PORT}",
                 hbaseSiteConf.get("random2.port"));
    ExecutionCommand cmd = hbr.getExecutionCommands().get(0);
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_log_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_pid_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_install_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_input_conf_dir"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_container_id"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("pid_file"));
    Assert.assertTrue(cmd.getConfigurations().get("global").containsKey("app_root"));
    Assert.assertTrue(cmd.getConfigurations().get("hbase-site").containsKey("defaultA"));
    Assert.assertFalse(cmd.getConfigurations().get("hbase-site").containsKey("defaultB"));
  }

  @Test
  public void testParameterParsing() throws IOException {
    AgentProviderService aps = createAgentProviderService(new Configuration());
    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getGlobalOptions().put(AgentKeys.SYSTEM_CONFIGS, "core-site,yarn-site, core-site ");
    List<String> configs = aps.getSystemConfigurationsRequested(treeOps);
    Assert.assertEquals(2, configs.size());
    Assert.assertTrue(configs.contains("core-site"));
    Assert.assertFalse(configs.contains("bore-site"));
  }

  @Test
  public void testDereferenceAllConfig() throws IOException {
    AgentProviderService aps = createAgentProviderService(new Configuration());
    Map<String, Map<String, String>> allConfigs = new HashMap<String, Map<String, String>>();
    Map<String, String> cfg1 = new HashMap<String, String>();
    cfg1.put("a1", "${@//site/cfg-2/A1}");
    cfg1.put("b1", "22");
    cfg1.put("c1", "33");
    cfg1.put("d1", "${@//site/cfg1/c1}AA");
    Map<String, String> cfg2 = new HashMap<String, String>();
    cfg2.put("A1", "11");
    cfg2.put("B1", "${@//site/cfg-2/A1},${@//site/cfg-2/A1},AA,${@//site/cfg1/c1}");
    cfg2.put("C1", "DD${@//site/cfg1/c1}");
    cfg2.put("D1", "${14}");

    allConfigs.put("cfg1", cfg1);
    allConfigs.put("cfg-2", cfg2);
    aps.dereferenceAllConfigs(allConfigs);
    Assert.assertEquals("11", cfg1.get("a1"));
    Assert.assertEquals("22", cfg1.get("b1"));
    Assert.assertEquals("33", cfg1.get("c1"));
    Assert.assertEquals("33AA", cfg1.get("d1"));

    Assert.assertEquals("11", cfg2.get("A1"));
    Assert.assertEquals("11,11,AA,33", cfg2.get("B1"));
    Assert.assertEquals("DD33", cfg2.get("C1"));
    Assert.assertEquals("${14}", cfg2.get("D1"));
  }

}
