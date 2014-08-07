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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.agent.application.metadata.Application;
import org.apache.slider.providers.agent.application.metadata.CommandOrder;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.Export;
import org.apache.slider.providers.agent.application.metadata.ExportGroup;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.MetainfoParser;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.apache.slider.server.appmaster.model.mock.MockFileSystem;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.CommandReport;
import org.apache.slider.server.appmaster.web.rest.agent.ComponentStatus;
import org.apache.slider.server.appmaster.web.rest.agent.ExecutionCommand;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
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
                                               + "          <name>HBASE_MASTER</name>\n"
                                               + "          <category>MASTER</category>\n"
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
                                               + "          <exports>\n"
                                               + "            <export>\n"
                                               + "              <name>PropertyA</name>\n"
                                               + "              <value>${THIS_HOST}:${site.global.listen_port}</value>\n"
                                               + "            </export>\n"
                                               + "            <export>\n"
                                               + "              <name>PropertyB</name>\n"
                                               + "              <value>AConstant</value>\n"
                                               + "            </export>\n"
                                               + "          </exports>\n"
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
  public void testRegistration() throws IOException {

    ConfTree tree = new ConfTree();
    tree.global.put(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");

    AgentProviderService aps = new AgentProviderService();
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
    FileSystem mockFs = new MockFileSystem();
    expect(sliderFileSystem.getFileSystem())
        .andReturn(new FilterFileSystem(mockFs)).anyTimes();
    expect(sliderFileSystem.createAmResource(anyObject(Path.class),
                                             anyObject(LocalResourceType.class)))
        .andReturn(createNiceMock(LocalResource.class)).anyTimes();
    expect(container.getId()).andReturn(new MockContainerId(1)).anyTimes();
    expect(container.getNodeId()).andReturn(new MockNodeId("localhost")).anyTimes();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);

    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();
    doReturn("scripts/hbase_master.py").when(mockAps).getScriptPathFromMetainfo(anyString());
    Metainfo metainfo = new Metainfo();
    metainfo.setApplication(new Application());
    doReturn(metainfo).when(mockAps).getApplicationMetainfo(any(SliderFileSystem.class), anyString());

    try {
      doReturn(true).when(mockAps).isMaster(anyString());
      doNothing().when(mockAps).addInstallCommand(
          eq("HBASE_MASTER"),
          eq("mockcontainer_1"),
          any(HeartBeatResponse.class),
          eq("scripts/hbase_master.py"));
    } catch (SliderException e) {
    }

    expect(access.isApplicationLive()).andReturn(true).anyTimes();
    ClusterDescription desc = new ClusterDescription();
    desc.setInfo(StatusKeys.INFO_AM_HOSTNAME, "host1");
    desc.setInfo(StatusKeys.INFO_AM_AGENT_PORT, "8088");
    desc.setInfo(StatusKeys.INFO_AM_SECURED_AGENT_PORT, "8089");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getClusterStatus()).andReturn(desc).anyTimes();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getOrAddComponent("HBASE_MASTER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    expect(access.getInstanceDefinitionSnapshot()).andReturn(aggConf);
    replay(access, ctx, container, sliderFileSystem);

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
    } catch (SliderException | IOException he) {
      log.warn("{}", he, he);
    }

    Register reg = new Register();
    reg.setResponseId(0);
    reg.setHostname("mockcontainer_1___HBASE_MASTER");
    RegistrationResponse resp = mockAps.handleRegistration(reg);
    Assert.assertEquals(0, resp.getResponseId());
    Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

    HeartBeat hb = new HeartBeat();
    hb.setResponseId(1);
    hb.setHostname("mockcontainer_1___HBASE_MASTER");
    HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
    Assert.assertEquals(2, hbr.getResponseId());
  }

  @Test
  public void testRoleHostMapping() throws Exception {
    AgentProviderService aps = new AgentProviderService();
    StateAccessForProviders appState = new ProviderAppState("undefined", null) {
      @Override
      public ClusterDescription getClusterStatus() {
        ClusterDescription cd = new ClusterDescription();
        cd.status = new HashMap<String, Object>();
        Map<String, Map<String, ClusterNode>> roleMap = new HashMap<>();
        ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
        cn1.host = "FIRST_HOST";
        Map<String, ClusterNode> map1 = new HashMap<>();
        map1.put("FIRST_CONTAINER", cn1);
        ClusterNode cn2 = new ClusterNode(new MockContainerId(2));
        cn2.host = "SECOND_HOST";
        Map<String, ClusterNode> map2 = new HashMap<>();
        map2.put("SECOND_CONTAINER", cn2);
        ClusterNode cn3 = new ClusterNode(new MockContainerId(3));
        cn3.host = "THIRD_HOST";
        map2.put("THIRD_CONTAINER", cn3);

        roleMap.put("FIRST_ROLE", map1);
        roleMap.put("SECOND_ROLE", map2);

        cd.status.put(ClusterDescriptionKeys.KEY_CLUSTER_LIVE, roleMap);

        return cd;
      }

      @Override
      public boolean isApplicationLive() {
        return true;
      }

      @Override
      public void refreshClusterStatus() {
        // do nothing
      }
    };

    aps.setAmState(appState);
    Map<String, String> tokens = new HashMap<>();
    aps.addRoleRelatedTokens(tokens);
    Assert.assertEquals(2, tokens.size());
    Assert.assertEquals("FIRST_HOST", tokens.get("${FIRST_ROLE_HOST}"));
    Assert.assertEquals("THIRD_HOST,SECOND_HOST", tokens.get("${SECOND_ROLE_HOST}"));
    aps.close();
  }

  @Test
  public void testComponentSpecificPublishes() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    AgentProviderService aps = new AgentProviderService();
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());
    doReturn(metainfo).when(mockAps).getMetainfo();

    Map<String, String> ports = new HashMap<>();
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
  public void testProcessConfig() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    Assert.assertNotNull(metainfo.getApplication());
    AgentProviderService aps = new AgentProviderService();
    HeartBeat hb = new HeartBeat();
    ComponentStatus status = new ComponentStatus();
    status.setClusterName("test");
    status.setComponentName("HBASE_MASTER");
    status.setRoleCommand("GET_CONFIG");
    Map<String, String> hbaseSite = new HashMap<>();
    hbaseSite.put("hbase.master.info.port", "60012");
    hbaseSite.put("c", "d");
    Map<String, Map<String, String>> configs = new HashMap<>();
    configs.put("hbase-site", hbaseSite);
    configs.put("global", hbaseSite);
    status.setConfigs(configs);
    hb.setComponentStatus(new ArrayList<>(Arrays.asList(status)));

    Map<String, Map<String, ClusterNode>> roleClusterNodeMap = new HashMap<>();
    Map<String, ClusterNode> container = new HashMap<>();
    ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
    cn1.host = "HOST1";
    container.put("cid1", cn1);
    roleClusterNodeMap.put("HBASE_MASTER", container);

    ComponentInstanceState componentStatus = new ComponentInstanceState("HBASE_MASTER", 
        new MockContainerId(1), "cid");
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());
    doReturn(metainfo).when(mockAps).getMetainfo();
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();

    mockAps.publishConfigAndExportGroups(hb, componentStatus);
    assert componentStatus.getConfigReported() == true;
    ArgumentCaptor<Collection> entriesCaptor = ArgumentCaptor.
        forClass(Collection.class);
    Mockito.verify(mockAps, Mockito.times(3)).publishApplicationInstanceData(
        anyString(),
        anyString(),
        entriesCaptor.capture());
    assert entriesCaptor.getAllValues().size() == 3;
    for (Collection coll : entriesCaptor.getAllValues()) {
      Set<Map.Entry<String, String>> entrySet = (Set<Map.Entry<String, String>>) coll;
      for (Map.Entry entry : entrySet) {
        log.info("{}:{}", entry.getKey(), entry.getValue().toString());
        if (entry.getKey().equals("JMX_Endpoint")) {
          assert entry.getValue().toString().equals("http://HOST1:60012/jmx");
        }
      }
    }
  }

  @Test
  public void testMetainfoParsing() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    Assert.assertNotNull(metainfo.getApplication());
    Application application = metainfo.getApplication();
    log.info("Service: " + application.toString());
    Assert.assertEquals(application.getName(), "HBASE");
    Assert.assertEquals(application.getExportedConfigs(), "hbase-site,global");
    Assert.assertEquals(application.getComponents().size(), 2);
    List<Component> components = application.getComponents();
    int found = 0;
    for (Component component : components) {
      if (component.getName().equals("HBASE_MASTER")) {
        Assert.assertEquals(component.getMinInstanceCount(), "1");
        Assert.assertEquals(component.getMaxInstanceCount(), "2");
        Assert.assertEquals(component.getCommandScript().getScript(), "scripts/hbase_master.py");
        Assert.assertEquals(component.getCategory(), "MASTER");
        Assert.assertEquals(component.getExports().size(), 0);
        found++;
      }
      if (component.getName().equals("HBASE_REGIONSERVER")) {
        Assert.assertEquals(component.getMinInstanceCount(), "1");
        Assert.assertNull(component.getMaxInstanceCount());
        Assert.assertEquals(component.getCommandScript().getScript(), "scripts/hbase_regionserver.py");
        Assert.assertEquals(component.getCategory(), "SLAVE");
        Assert.assertEquals(component.getExports().size(), 2);
        List<Export> es = component.getExports();
        Export e = es.get(0);
        Assert.assertEquals(e.getName(), "PropertyA");
        Assert.assertEquals(e.getValue(), "${THIS_HOST}:${site.global.listen_port}");
        e = es.get(1);
        Assert.assertEquals(e.getName(), "PropertyB");
        Assert.assertEquals(e.getValue(), "AConstant");
        found++;
      }
    }
    Assert.assertEquals(found, 2);

    Assert.assertEquals(application.getExportGroups().size(), 1);
    List<ExportGroup> egs = application.getExportGroups();
    ExportGroup eg = egs.get(0);
    Assert.assertEquals(eg.getName(), "QuickLinks");
    Assert.assertEquals(eg.getExports().size(), 2);

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

    List<CommandOrder> cmdOrders = application.getCommandOrder();
    Assert.assertEquals(cmdOrders.size(), 2);
    found = 0;
    for (CommandOrder co : application.getCommandOrder()) {
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

    AgentProviderService aps = new AgentProviderService();
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(metainfo).when(mockAps).getMetainfo();
    String scriptPath = mockAps.getScriptPathFromMetainfo("HBASE_MASTER");
    Assert.assertEquals(scriptPath, "scripts/hbase_master.py");

    String metainfo_1_str_bad = "<metainfo>\n"
                                + "  <schemaVersion>2.0</schemaVersion>\n"
                                + "  <services>\n"
                                + "    <service>\n"
                                + "      <name>HBASE</name>\n"
                                + "      <comment>\n"
                                + "        Apache HBase\n"
                                + "      </comment>\n";

    metainfo_1 = new ByteArrayInputStream(metainfo_1_str_bad.getBytes());
    metainfo = new MetainfoParser().parse(metainfo_1);
    Assert.assertNull(metainfo);
  }

  @Test
  public void testMetaInfoRelatedOperations() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    InputStream metainfo_2 = new ByteArrayInputStream(metainfo_2_str.getBytes());
    Metainfo metainfo2 = new MetainfoParser().parse(metainfo_2);
    String role_hm = "HBASE_MASTER";
    String role_hrs = "HBASE_REGIONSERVER";

    AgentProviderService aps = new AgentProviderService();
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(metainfo).when(mockAps).getMetainfo();

    AgentProviderService mockAps2 = Mockito.spy(aps);
    doReturn(metainfo2).when(mockAps2).getMetainfo();

    Assert.assertTrue(mockAps.isMaster(role_hm));
    Assert.assertFalse(mockAps.isMaster(role_hrs));
    Assert.assertFalse(mockAps.canPublishConfig(role_hm));
    Assert.assertFalse(mockAps.canPublishConfig(role_hrs));
    Assert.assertFalse(mockAps.canAnyMasterPublishConfig());

    Assert.assertTrue(mockAps2.isMaster(role_hm));
    Assert.assertFalse(mockAps2.isMaster(role_hrs));
    Assert.assertTrue(mockAps2.canPublishConfig(role_hm));
    Assert.assertFalse(mockAps2.canPublishConfig(role_hrs));
    Assert.assertTrue(mockAps2.canAnyMasterPublishConfig());
  }

  @Test
  public void testOrchestratedAppStart() throws IOException {
    // App has two components HBASE_MASTER and HBASE_REGIONSERVER
    // Start of HBASE_RS depends on the start of HBASE_MASTER
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    ConfTree tree = new ConfTree();
    tree.global.put(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH, ".");

    AgentProviderService aps = new AgentProviderService();
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
    Path generatedConfPath = new Path(".", "test");
    MapOperations resourceComponent = new MapOperations();
    MapOperations appComponent = new MapOperations();
    Path containerTmpDirPath = new Path(".", "test");
    FileSystem mockFs = new MockFileSystem();
    expect(sliderFileSystem.getFileSystem())
        .andReturn(new FilterFileSystem(mockFs)).anyTimes();
    expect(sliderFileSystem.createAmResource(anyObject(Path.class),
                                             anyObject(LocalResourceType.class)))
        .andReturn(createNiceMock(LocalResource.class)).anyTimes();
    expect(container.getId()).andReturn(new MockContainerId(1)).anyTimes();
    expect(container.getNodeId()).andReturn(new MockNodeId("localhost")).anyTimes();
    StateAccessForProviders access = createNiceMock(StateAccessForProviders.class);

    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(access).when(mockAps).getAmState();
    doReturn(metainfo).when(mockAps).getApplicationMetainfo(any(SliderFileSystem.class), anyString());

    try {
      doReturn(true).when(mockAps).isMaster(anyString());
      doNothing().when(mockAps).addInstallCommand(
          anyString(),
          anyString(),
          any(HeartBeatResponse.class),
          anyString());
      doNothing().when(mockAps).addStartCommand(
          anyString(),
          anyString(),
          any(HeartBeatResponse.class),
          anyString());
      doNothing().when(mockAps).addGetConfigCommand(
          anyString(),
          anyString(),
          any(HeartBeatResponse.class));
      doNothing().when(mockAps).publishApplicationInstanceData(
          anyString(),
          anyString(),
          anyCollection());

    } catch (SliderException e) {
    }

    expect(access.isApplicationLive()).andReturn(true).anyTimes();
    ClusterDescription desc = new ClusterDescription();
    desc.setInfo(StatusKeys.INFO_AM_HOSTNAME, "host1");
    desc.setInfo(StatusKeys.INFO_AM_AGENT_PORT, "8088");
    desc.setInfo(StatusKeys.INFO_AM_SECURED_AGENT_PORT, "8089");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    expect(access.getClusterStatus()).andReturn(desc).anyTimes();

    AggregateConf aggConf = new AggregateConf();
    ConfTreeOperations treeOps = aggConf.getAppConfOperations();
    treeOps.getOrAddComponent("HBASE_MASTER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    treeOps.getOrAddComponent("HBASE_REGIONSERVER").put(AgentKeys.WAIT_HEARTBEAT, "0");
    expect(access.getInstanceDefinitionSnapshot()).andReturn(aggConf).anyTimes();
    replay(access, ctx, container, sliderFileSystem);

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

      mockAps.buildContainerLaunchContext(launcher,
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
      reg.setHostname("mockcontainer_1___HBASE_MASTER");
      RegistrationResponse resp = mockAps.handleRegistration(reg);
      Assert.assertEquals(0, resp.getResponseId());
      Assert.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

      reg = new Register();
      reg.setResponseId(0);
      reg.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
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
                                                                  anyString());

      hb = new HeartBeat();
      hb.setResponseId(1);
      hb.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(2, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(2)).addInstallCommand(anyString(),
                                                                  anyString(),
                                                                  any(HeartBeatResponse.class),
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
                                                                anyString());
      // RS still does not start
      hb = new HeartBeat();
      hb.setResponseId(3);
      hb.setHostname("mockcontainer_1___HBASE_REGIONSERVER");
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(4, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(0)).addStartCommand(anyString(),
                                                                anyString(),
                                                                any(HeartBeatResponse.class),
                                                                anyString());

      // MASTER succeeds install and issues start
      hb = new HeartBeat();
      hb.setResponseId(2);
      hb.setHostname("mockcontainer_1___HBASE_MASTER");
      cr = new CommandReport();
      cr.setRole("HBASE_MASTER");
      cr.setRoleCommand("INSTALL");
      cr.setStatus("COMPLETED");
      Map<String, String> ap = new HashMap<>();
      ap.put("a.port", "10233");
      cr.setAllocatedPorts(ap);
      hb.setReports(Arrays.asList(cr));
      hbr = mockAps.handleHeartBeat(hb);
      Assert.assertEquals(3, hbr.getResponseId());
      Mockito.verify(mockAps, Mockito.times(1)).addStartCommand(anyString(),
                                                                anyString(),
                                                                any(HeartBeatResponse.class),
                                                                anyString());
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
                                                                anyString());
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
                                                                anyString());
    } catch (SliderException | IOException he) {
      log.warn(he.getMessage());
    }

    Mockito.verify(mockAps, Mockito.times(1)).publishApplicationInstanceData(
        anyString(),
        anyString(),
        anyCollection());
  }

  @Test
  public void testNotifyContainerCompleted() {
    AgentProviderService aps = new AgentProviderService();
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishApplicationInstanceData(anyString(), anyString(), anyCollection());

    ContainerId cid = new MockContainerId(1);
    String id = cid.toString();
    ContainerId cid2 = new MockContainerId(2);
    mockAps.getAllocatedPorts().put("a", "100");
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
    Assert.assertEquals(mockAps.getAllocatedPorts(id).size(), 1);
    Assert.assertEquals(mockAps.getAllocatedPorts("cid2").size(), 1);

    // Make the call
    mockAps.notifyContainerCompleted(new MockContainerId(1));

    Assert.assertEquals(mockAps.getAllocatedPorts().size(), 1);
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
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    AgentProviderService aps = new AgentProviderService();
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
    doReturn(metainfo).when(mockAps).getMetainfo();

    Map<String, Map<String, ClusterNode>> roleClusterNodeMap = new HashMap<>();
    Map<String, ClusterNode> container = new HashMap<>();
    ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
    cn1.host = "HOST1";
    container.put("cid1", cn1);
    roleClusterNodeMap.put("HBASE_MASTER", container);
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();

    replay(access);

    mockAps.addInstallCommand("HBASE_MASTER", "cid1", hbr, "");
    ExecutionCommand cmd = hbr.getExecutionCommands().get(0);
    String pkgs = cmd.getHostLevelParams().get(AgentKeys.PACKAGE_LIST);
    Assert.assertEquals("[{\"type\":\"tarball\",\"name\":\"files/hbase-0.96.1-hadoop2-bin.tar.gz\"}]", pkgs);
    Assert.assertEquals("java_home", cmd.getHostLevelParams().get(AgentKeys.JAVA_HOME));
    Assert.assertEquals("cid1", cmd.getHostLevelParams().get("container_id"));
    Assert.assertEquals(Command.INSTALL.toString(), cmd.getRoleCommand());
  }

  @Test
  public void testAddStartCommand() throws Exception {
    AgentProviderService aps = new AgentProviderService();
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
    treeOps.set("config_types", "hbase-site");
    treeOps.getGlobalOptions().put("site.hbase-site.a.port", "${HBASE_MASTER.ALLOCATED_PORT}");
    treeOps.getGlobalOptions().put("site.hbase-site.b.port", "${HBASE_MASTER.ALLOCATED_PORT}");
    treeOps.getGlobalOptions().put("site.hbase-site.random.port", "${HBASE_MASTER.ALLOCATED_PORT}{DO_NOT_PROPAGATE}");
    treeOps.getGlobalOptions().put("site.hbase-site.random2.port", "${HBASE_MASTER.ALLOCATED_PORT}");

    expect(access.getAppConfSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.getInternalsSnapshot()).andReturn(treeOps).anyTimes();
    expect(access.isApplicationLive()).andReturn(true).anyTimes();

    doReturn("HOST1").when(mockAps).getClusterInfoPropertyValue(anyString());

    Map<String, Map<String, ClusterNode>> roleClusterNodeMap = new HashMap<>();
    Map<String, ClusterNode> container = new HashMap<>();
    ClusterNode cn1 = new ClusterNode(new MockContainerId(1));
    cn1.host = "HOST1";
    container.put("cid1", cn1);
    roleClusterNodeMap.put("HBASE_MASTER", container);
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();
    Map<String, String> allocatedPorts = new HashMap<>();
    allocatedPorts.put("hbase-site.a.port", "10023");
    allocatedPorts.put("hbase-site.b.port", "10024");
    doReturn(allocatedPorts).when(mockAps).getAllocatedPorts();
    Map<String, String> allocatedPorts2 = new HashMap<>();
    allocatedPorts2.put("hbase-site.random.port", "10025");
    doReturn(allocatedPorts2).when(mockAps).getAllocatedPorts(anyString());

    replay(access);

    mockAps.addStartCommand("HBASE_MASTER", "cid1", hbr, "");
    Assert.assertTrue(hbr.getExecutionCommands().get(0).getConfigurations().containsKey("hbase-site"));
    Map<String, String> hbaseSiteConf = hbr.getExecutionCommands().get(0).getConfigurations().get("hbase-site");
    Assert.assertTrue(hbaseSiteConf.containsKey("a.port"));
    Assert.assertEquals("10023", hbaseSiteConf.get("a.port"));
    Assert.assertEquals("10024", hbaseSiteConf.get("b.port"));
    Assert.assertEquals("10025", hbaseSiteConf.get("random.port"));
    assertEquals("${HBASE_MASTER.ALLOCATED_PORT}",
        hbaseSiteConf.get("random2.port"));
  }

}
