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
package org.apache.slider.server.appmaster.web.view

import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.Injector
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR
import org.apache.hadoop.yarn.webapp.hamlet.HamletImpl.EImp
import org.apache.slider.api.ClusterNode
import org.apache.slider.providers.ProviderService
import org.apache.slider.server.appmaster.model.mock.*
import org.apache.slider.server.appmaster.state.ProviderAppState
import org.apache.slider.server.appmaster.state.RoleInstance
import org.apache.slider.server.appmaster.web.WebAppApi
import org.apache.slider.server.appmaster.web.WebAppApiImpl
import org.apache.slider.server.appmaster.web.view.ContainerStatsBlock.ClusterNodeNameComparator
import org.apache.slider.server.appmaster.web.view.ContainerStatsBlock.TableAnchorContent
import org.apache.slider.server.appmaster.web.view.ContainerStatsBlock.TableContent
import org.junit.Before
import org.junit.Test

@Slf4j
//@CompileStatic
public class TestContainerStatsBlock extends BaseMockAppStateTest {

  private ContainerStatsBlock statsBlock;


  private Container cont1, cont2;

  @Before
  public void setup() {
    super.setup()
    ProviderService providerService = new MockProviderService();
    ProviderAppState providerAppState = new ProviderAppState(
        "undefined",
        appState)

    WebAppApiImpl inst = new WebAppApiImpl(
        providerAppState,
        providerService,
        null,
        null, metrics, null, null, null);

    Injector injector = Guice.createInjector(new WebappModule(inst))
    statsBlock = injector.getInstance(ContainerStatsBlock.class);

    cont1 = new MockContainer();

    cont1.id = mockContainerId(0);
    cont1.nodeId = new MockNodeId();
    cont1.priority = Priority.newInstance(1);
    cont1.resource = new MockResource();

    cont2 = new MockContainer();
    cont2.id = mockContainerId(1);
    cont2.nodeId = new MockNodeId();
    cont2.priority = Priority.newInstance(1);
    cont2.resource = new MockResource();
  }

  
  public static class WebappModule extends AbstractModule {
    final WebAppApiImpl instance;

    WebappModule(WebAppApiImpl instance) {
      this.instance = instance
    }

    @Override
    protected void configure() {
      bind(WebAppApi.class).toInstance(instance);
    }
  }
  
  
  public MockContainerId mockContainerId(int count) {
    new MockContainerId(applicationAttemptId, count)
  }

  @Test
  public void testGetContainerInstances() {
    List<RoleInstance> roles = [
      new RoleInstance(cont1),
      new RoleInstance(cont2),
    ];
    Map<String, RoleInstance> map = statsBlock.getContainerInstances(roles);

    assert 2 == map.size();

    assert map.containsKey("mockcontainer_0");
    assert map.get("mockcontainer_0").equals(roles[0]);

    assert map.containsKey("mockcontainer_1");
    assert map.get("mockcontainer_1").equals(roles[1]);
  }

  @Test
  public void testGenerateRoleDetailsWithTwoColumns() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    
    // Make a div to put the content into
    DIV<Hamlet> div = hamlet.div();
    
    String detailsName = "testing";
    String selector = "selector";
    Map<TableContent,String> data = new HashMap<TableContent,String>();
    data.put(new ContainerStatsBlock.TableContent("Foo"), "bar");
    
    int levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());
    
    // Close out the div we made
    // DIV<Hamlet>._() will actually invoke the wrong method (creating <p>), explicit 
    // cast to make sure we're closing out the <div>
    ((EImp) div)._();
    
    assert levelPrior == hamlet.nestLevel();
  }
  
  @Test
  public void testGenerateRoleDetailsWithOneColumn() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    DIV<Hamlet> div = hamlet.div();
    
    String detailsName = "testing";
    String selector = "selector";
    Map<TableContent,String> data = new HashMap<TableContent,String>();
    data.put(new ContainerStatsBlock.TableContent("Bar"), null);
    
    int levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());
    
    // Close out the div we made
    // DIV<Hamlet>._() will actually invoke the wrong method (creating <p>), explicit 
    // cast to make sure we're closing out the <div>
    ((EImp) div)._();
    
    assert levelPrior == hamlet.nestLevel();
  }
  
  @Test
  public void testGenerateRoleDetailsWithNoData() {
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);

    Hamlet hamlet = new Hamlet(pw, 0, false);
    DIV<Hamlet> div = hamlet.div();
    
    String detailsName = "testing";
    String selector = "selector";
    Map<TableContent,String> data = new HashMap<TableContent,String>();
    
    int levelPrior = hamlet.nestLevel();
    statsBlock.generateRoleDetails(div, selector, detailsName, data.entrySet());
    
    // Close out the div we made
    // DIV<Hamlet>._() will actually invoke the wrong method (creating <p>), explicit 
    // cast to make sure we're closing out the <div>
    ((EImp) div)._();
    
    assert levelPrior == hamlet.nestLevel();
  }
  
  @Test
  public void testClusterNodeNameComparator() {
    ClusterNode n1 = new ClusterNode(mockContainerId(1)),
      n2 = new ClusterNode(mockContainerId(2)),
      n3 = new ClusterNode(mockContainerId(3));
    
    List<ClusterNode> nodes = new ArrayList<ClusterNode>();
    nodes.add(n2);
    nodes.add(n3);
    nodes.add(n1);
    
    Collections.sort(nodes, new ClusterNodeNameComparator());
    
    String prevName = "";
    for (ClusterNode node : nodes) {
      assert prevName.compareTo(node.name) <= 0;
      prevName = node.name;
    }
  }
  
  @Test
  public void testTableContent() { 
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);
    TableContent tc = new TableContent("foo");
    
    Hamlet hamlet = new Hamlet(pw, 0, false);
    TR<TABLE<Hamlet>> tr = hamlet.table().tr();
    
    int prevLevel = hamlet.nestLevel();
    // printCell should not end the tr
    tc.printCell(tr);
    tr._();
    assert prevLevel == hamlet.nestLevel();
  }
  
  @Test
  public void testTableAnchorContent() { 
    StringWriter sw = new StringWriter(64);
    PrintWriter pw = new PrintWriter(sw);
    TableContent tc = new TableAnchorContent("foo", "http://bar.com");
    
    Hamlet hamlet = new Hamlet(pw, 0, false);
    TR<TABLE<Hamlet>> tr = hamlet.table().tr();
    
    int prevLevel = hamlet.nestLevel();
    // printCell should not end the tr
    tc.printCell(tr);
    tr._();
    assert prevLevel == hamlet.nestLevel();
  }
}