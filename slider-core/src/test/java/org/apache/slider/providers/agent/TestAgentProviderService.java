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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
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
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.MetainfoParser;
import org.apache.slider.providers.agent.application.metadata.Service;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.apache.slider.server.appmaster.model.mock.MockFileSystem;
import org.apache.slider.server.appmaster.model.mock.MockNodeId;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;


public class TestAgentProviderService {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAgentProviderService.class);

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
    aps.setRoleHostMapping("FIRST_ROLE", "FIRST_HOST");
    aps.setRoleHostMapping("SECOND_ROLE", "SECOND_HOST");
    aps.setRoleHostMapping("SECOND_ROLE", "THIRD_HOST");
    Map<String, String> tokens = new HashMap<String, String>();
    aps.addRoleRelatedTokens(tokens);
    TestCase.assertEquals(2, tokens.size());
    TestCase.assertEquals("FIRST_HOST", tokens.get("${FIRST_ROLE_HOST}"));
    TestCase.assertEquals("SECOND_HOST,THIRD_HOST", tokens.get("${SECOND_ROLE_HOST}"));
    aps.close();
  }

  @Test
  public void testMetainfoParsing() throws Exception {
    String metainfo_1_str = "<metainfo>\n"
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

    InputStream metainfo_1 = new ByteArrayInputStream(metainfo_1_str.getBytes());
    Metainfo metainfo = new MetainfoParser().parse(metainfo_1);
    assert metainfo.getServices().size() == 1;
    Service service = metainfo.getServices().get(0);
    log.info("Service: " + service.toString());
    assert service.getName().equals("HBASE");
    assert service.getComponents().size() == 2;
    List<Component> components = service.getComponents();
    for (Component component : components) {
      if (component.getName().equals("HBASE_MASTER")) {
        assert component.getCommandScript().getScript().equals("scripts/hbase_master.py");
        assert component.getCategory().equals("MASTER");
      }
      if (component.getName().equals("HBASE_REGIONSERVER")) {
        assert component.getCommandScript().getScript().equals("scripts/hbase_regionserver.py");
        assert component.getCategory().equals("SLAVE");
      }
    }

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
}
