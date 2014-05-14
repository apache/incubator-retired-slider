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

import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
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
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.Export;
import org.apache.slider.providers.agent.application.metadata.ExportGroup;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.MetainfoParser;
import org.apache.slider.providers.agent.application.metadata.Service;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.apache.slider.server.appmaster.model.mock.MockFileSystem;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.ComponentStatus;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
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
                                               + "  <services>\n"
                                               + "    <service>\n"
                                               + "      <name>HBASE</name>\n"
                                               + "      <comment>\n"
                                               + "        Apache HBase\n"
                                               + "      </comment>\n"
                                               + "      <version>0.96.0.2.1.1</version>\n"
                                               + "      <type>YARN-APP</type>\n"
                                               + "      <minHadoopVersion>2.1.0</minHadoopVersion>\n"
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
                                               + "    </service>\n"
                                               + "  </services>\n"
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
    doReturn(access).when(mockAps).getStateAccessor();
    doReturn("scripts/hbase_master.py").when(mockAps).getScriptPathFromMetainfo(anyString());
    doReturn(new Metainfo()).when(mockAps).getApplicationMetainfo(any(SliderFileSystem.class), anyString());

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
    desc.setInfo(StatusKeys.INFO_AM_WEB_PORT, "8088");
    desc.setInfo(OptionKeys.APPLICATION_NAME, "HBASE");
    desc.getOrAddRole("HBASE_MASTER").put(AgentKeys.COMPONENT_SCRIPT, "scripts/hbase_master.py");
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
    } catch (SliderException he) {
      log.warn(he.getMessage());
    } catch (IOException ioe) {
      log.warn(ioe.getMessage());
    }

    Register reg = new Register();
    reg.setResponseId(0);
    reg.setHostname("mockcontainer_1___HBASE_MASTER");
    RegistrationResponse resp = mockAps.handleRegistration(reg);
    TestCase.assertEquals(0, resp.getResponseId());
    TestCase.assertEquals(RegistrationStatus.OK, resp.getResponseStatus());

    HeartBeat hb = new HeartBeat();
    hb.setResponseId(1);
    hb.setHostname("mockcontainer_1___HBASE_MASTER");
    HeartBeatResponse hbr = mockAps.handleHeartBeat(hb);
    TestCase.assertEquals(2, hbr.getResponseId());
  }

  @Test
  public void testRoleHostMapping() throws Exception {
    AgentProviderService aps = new AgentProviderService();
    StateAccessForProviders appState = new AppState(null) {
      @Override
      public Map<ContainerId, RoleInstance> getLiveNodes() {
        Map<ContainerId, RoleInstance> retVal = new HashMap<>();
        ContainerId cid = new MyContainerId(1);
        Container container = new MyContainer();
        container.setId(cid);
        RoleInstance ri = new RoleInstance(container);
        ri.host = "FIRST_HOST";
        ri.role = "FIRST_ROLE";
        retVal.put(cid, ri);

        cid = new MyContainerId(2);
        container = new MyContainer();
        container.setId(cid);
        ri = new RoleInstance(container);
        ri.host = "SECOND_HOST";
        ri.role = "SECOND_ROLE";
        retVal.put(cid, ri);

        cid = new MyContainerId(3);
        container = new MyContainer();
        container.setId(cid);
        ri = new RoleInstance(container);
        ri.host = "THIRD_HOST";
        ri.role = "SECOND_ROLE";
        retVal.put(cid, ri);

        return retVal;
      }

      @Override
      public boolean isApplicationLive() {
        return true;
      }
    };

    aps.setStateAccessor(appState);
    Map<String, String> tokens = new HashMap<>();
    aps.addRoleRelatedTokens(tokens);
    TestCase.assertEquals(2, tokens.size());
    TestCase.assertEquals("FIRST_HOST", tokens.get("${FIRST_ROLE_HOST}"));
    TestCase.assertEquals("SECOND_HOST,THIRD_HOST", tokens.get("${SECOND_ROLE_HOST}"));
    aps.close();
  }

  @Test
  public void testProcessConfig() throws Exception {
    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    assert metainfo.getServices().size() == 1;
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

    Map<String, Map<String, String>> roleClusterNodeMap = new HashMap<>();
    Map<String, String> container = new HashMap<>();
    container.put("cid1", "HOST1");
    roleClusterNodeMap.put("HBASE_MASTER", container);

    ComponentInstanceState componentStatus = new ComponentInstanceState("HBASE_MASTER", "aid", "cid");
    AgentProviderService mockAps = Mockito.spy(aps);
    doNothing().when(mockAps).publishComponentConfiguration(anyString(), anyString(), anyCollection());
    doReturn(metainfo).when(mockAps).getMetainfo();
    doReturn(roleClusterNodeMap).when(mockAps).getRoleClusterNodeMapping();

    mockAps.processReturnedStatus(hb, componentStatus);
    assert componentStatus.getConfigReported() == true;
    ArgumentCaptor<Collection> commandCaptor = ArgumentCaptor.
        forClass(Collection.class);
    Mockito.verify(mockAps, Mockito.times(3)).publishComponentConfiguration(
        anyString(),
        anyString(),
        commandCaptor.capture());
    assert commandCaptor.getAllValues().size() == 3;
    for (Collection coll : commandCaptor.getAllValues()) {
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
    assert metainfo.getServices().size() == 1;
    Service service = metainfo.getServices().get(0);
    log.info("Service: " + service.toString());
    assert service.getName().equals("HBASE");
    assert service.getComponents().size() == 2;
    List<Component> components = service.getComponents();
    int found = 0;
    for (Component component : components) {
      if (component.getName().equals("HBASE_MASTER")) {
        assert component.getCommandScript().getScript().equals("scripts/hbase_master.py");
        assert component.getCategory().equals("MASTER");
        found++;
      }
      if (component.getName().equals("HBASE_REGIONSERVER")) {
        assert component.getCommandScript().getScript().equals("scripts/hbase_regionserver.py");
        assert component.getCategory().equals("SLAVE");
        found++;
      }
    }
    assert found == 2;

    assert service.getExportGroups().size() == 1;
    List<ExportGroup> egs = service.getExportGroups();
    ExportGroup eg = egs.get(0);
    assert eg.getName().equals("QuickLinks");
    assert eg.getExports().size() == 2;

    found = 0;
    for (Export export : eg.getExports()) {
      if (export.getName().equals("JMX_Endpoint")) {
        found++;
        assert export.getValue().equals(
            "http://${HBASE_MASTER_HOST}:${site.hbase-site.hbase.master.info.port}/jmx");
      }
      if (export.getName().equals("Master_Status")) {
        found++;
        assert export.getValue().equals(
            "http://${HBASE_MASTER_HOST}:${site.hbase-site.hbase.master.info.port}/master-status");
      }
    }
    assert found == 2;

    AgentProviderService aps = new AgentProviderService();
    AgentProviderService mockAps = Mockito.spy(aps);
    doReturn(metainfo).when(mockAps).getMetainfo();
    String scriptPath = mockAps.getScriptPathFromMetainfo("HBASE_MASTER");
    assert scriptPath.equals("scripts/hbase_master.py");

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
    assert metainfo == null;
  }

  private static class MyContainer extends Container {

    ContainerId cid = null;

    @Override
    public ContainerId getId() {
      return this.cid;
    }

    @Override
    public void setId(ContainerId containerId) {
      this.cid = containerId;
    }

    @Override
    public NodeId getNodeId() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNodeId(NodeId nodeId) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getNodeHttpAddress() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setNodeHttpAddress(String s) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Resource getResource() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setResource(Resource resource) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Priority getPriority() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setPriority(Priority priority) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Token getContainerToken() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setContainerToken(Token token) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int compareTo(Container o) {
      return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
  }

  private static class MyContainerId extends ContainerId {
    int id;

    private MyContainerId(int id) {
      this.id = id;
    }

    @Override
    public ApplicationAttemptId getApplicationAttemptId() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getId() {
      return id;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void setId(int i) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void build() {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int hashCode() {
      return this.id;
    }

    @Override
    public String toString() {
      return "MyContainerId{" +
             "id=" + id +
             '}';
    }
  }
}
