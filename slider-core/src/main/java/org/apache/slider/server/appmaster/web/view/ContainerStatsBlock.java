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
package org.apache.slider.server.appmaster.web.view;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TR;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.client.SliderClusterOperations;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 
 */
public class ContainerStatsBlock extends HtmlBlock {
  private static final Logger log = LoggerFactory.getLogger(ContainerStatsBlock.class);

  private static final String EVEN = "even", ODD = "odd", BOLD = "bold", SCHEME = "http://", PATH = "/node/container/";

  // Some functions that help transform the data into an object we can use to abstract presentation specifics
  protected static final Function<Entry<String,Integer>,Entry<TableContent,Integer>> stringIntPairFunc = toTableContentFunction();
  protected static final Function<Entry<String,String>,Entry<TableContent,String>> stringStringPairFunc = toTableContentFunction();

  private WebAppApi slider;
  private SliderClusterOperations clusterOps;

  @Inject
  public ContainerStatsBlock(WebAppApi slider) {
    this.slider = slider;
    clusterOps = new SliderClusterOperations(slider.getClusterProtocol());
  }

  /**
   * Sort a collection of ClusterNodes by name
   */
  protected static class ClusterNodeNameComparator implements Comparator<ClusterNode> {

    @Override
    public int compare(ClusterNode node1, ClusterNode node2) {
      if (null == node1 && null != node2) {
        return -1;
      } else if (null != node1 && null == node2) {
        return 1;
      } else if (null == node1 && null == node2) {
        return 0;
      }

      final String name1 = node1.name, name2 = node2.name;
      if (null == name1 && null != name2) {
        return -1;
      } else if (null != name1 && null == name2) {
        return 1;
      } else if (null == name1 && null == name2) {
        return 0;
      }

      return name1.compareTo(name2);
    }

  }

  @Override
  protected void render(Block html) {
    // TODO Probably better to just get a copy of this list for us to avoid the repeated synchronization?
    // does this change if we have 50 node, 100node, 500 node clusters?
    final Map<String,RoleInstance> containerInstances = getContainerInstances(slider.getAppState().cloneActiveContainerList());

    for (Entry<String,RoleStatus> entry : slider.getRoleStatusByName().entrySet()) {
      final String name = entry.getKey();
      final RoleStatus roleStatus = entry.getValue();

      DIV<Hamlet> div = html.div("role-info ui-widget-content ui-corner-all");

      List<ClusterNode> nodesInRole;
      try {
        nodesInRole = clusterOps.listClusterNodesInRole(name);
      } catch (Exception e) {
        log.error("Could not fetch containers for role: " + name, e);
        nodesInRole = Collections.emptyList();
      }

      div.h2(BOLD, StringUtils.capitalize(name));

      // Generate the details on this role
      Iterable<Entry<String,Integer>> stats = roleStatus.buildStatistics().entrySet();
      generateRoleDetails(div,"role-stats-wrap", "Specifications", Iterables.transform(stats, stringIntPairFunc));

      // Sort the ClusterNodes by their name (containerid)
      Collections.sort(nodesInRole, new ClusterNodeNameComparator());

      // Generate the containers running this role
      generateRoleDetails(div, "role-stats-containers", "Containers",
          Iterables.transform(nodesInRole, new Function<ClusterNode,Entry<TableContent,String>>() {

            @Override
            public Entry<TableContent,String> apply(ClusterNode input) {
              final String containerId = input.name;
              
              if (containerInstances.containsKey(containerId)) {
                RoleInstance roleInst = containerInstances.get(containerId);
                if (roleInst.container.getNodeHttpAddress() != null) {
                  return Maps.<TableContent,String> immutableEntry(
                    new TableAnchorContent(containerId, buildNodeUrlForContainer(roleInst.container.getNodeHttpAddress(), containerId)), null);
                }
              }
              return Maps.immutableEntry(new TableContent(input.name), null);
            }

          }));

      ClusterDescription desc = slider.getAppState().getClusterStatus();
      Map<String,String> options = desc.getRole(name);
      Iterable<Entry<TableContent,String>> tableContent;
      
      // Generate the pairs of data in the expected form
      if (null != options) {
        tableContent = Iterables.transform(options.entrySet(), stringStringPairFunc);
      } else {
        // Or catch that we have no options and provide "empty"
        tableContent = Collections.<Entry<TableContent,String>> emptySet();
      }
      
      // Generate the options used by this role
      generateRoleDetails(div, "role-options-wrap", "Role Options", tableContent);

      // Close the div for this role
      div._();
    }
  }

  protected static <T> Function<Entry<String,T>,Entry<TableContent,T>> toTableContentFunction() {
    return new Function<Entry<String,T>,Entry<TableContent,T>>() {
      @Override
      public Entry<TableContent,T> apply(Entry<String,T> input) {
        return Maps.immutableEntry(new TableContent(input.getKey()), input.getValue());
      }
    };
  }

  protected Map<String,RoleInstance> getContainerInstances(List<RoleInstance> roleInstances) {
    Map<String,RoleInstance> map = Maps.newHashMapWithExpectedSize(roleInstances.size());
    for (RoleInstance roleInstance : roleInstances) {
      // UUID is the containerId
      map.put(roleInstance.id, roleInstance);
    }
    return map;
  }

  /**
   * Given a div, a name for this data, and some pairs of data, generate a nice HTML table. If contents is empty (of size zero), then a mesage will be printed
   * that there were no items instead of an empty table.
   * 
   * @param div
   * @param detailsName
   * @param contents
   */
  protected <T1 extends TableContent,T2> void generateRoleDetails(DIV<Hamlet> parent, String divSelector, String detailsName, Iterable<Entry<T1,T2>> contents) {
    final DIV<DIV<Hamlet>> div = parent.div(divSelector).h3(BOLD, detailsName);

    int offset = 0;
    TABLE<DIV<DIV<Hamlet>>> table = null;
    TBODY<TABLE<DIV<DIV<Hamlet>>>> tbody = null;
    for (Entry<T1,T2> content : contents) {
      if (null == table) {
        table = div.table("ui-widget-content ui-corner-bottom");
        tbody = table.tbody();
      }
      
      TR<TBODY<TABLE<DIV<DIV<Hamlet>>>>> row = tbody.tr(offset % 2 == 0 ? EVEN : ODD);
      
      // Defer to the implementation of the TableContent for what the cell should contain
      content.getKey().printCell(row);

      // Only add the second column if the element is non-null
      // This also lets us avoid making a second method if we're only making a one-column table
      if (null != content.getValue()) {
        row.td(content.getValue().toString());
      }

      row._();

      offset++;
    }

    // If we made a table, close it out
    if (null != table) {
      tbody._()._();
    } else {
      // Otherwise, throw in a nice "no content" message
      div.p("no-table-contents")._("None")._();
    }
    
    // Close out the initial div
    div._();
  }

  /**
   * Build a URL from the address:port and container ID directly to the NodeManager service
   * @param nodeAddress
   * @param containerId
   * @return
   */
  protected String buildNodeUrlForContainer(String nodeAddress, String containerId) {
    StringBuilder sb = new StringBuilder(SCHEME.length() + nodeAddress.length() + PATH.length() + containerId.length());

    sb.append(SCHEME).append(nodeAddress).append(PATH).append(containerId);

    return sb.toString();
  }

  /**
   * Creates a table cell with the provided String as content.
   */
  protected static class TableContent {
    private String cell;

    public TableContent(String cell) {
      this.cell = cell;
    }

    public String getCell() {
      return cell;
    }

    /**
     * Adds a td to the given tr. The tr is not closed 
     * @param tableRow
     */
    public void printCell(TR<?> tableRow) {
      tableRow.td(this.cell);
    }
  }

  /**
   * Creates a table cell with an anchor to the given URL with the provided String as content.
   */
  protected static class TableAnchorContent extends TableContent {
    private String anchorUrl;

    public TableAnchorContent(String cell, String anchorUrl) {
      super(cell);
      this.anchorUrl = anchorUrl;
    }

    /* (non-javadoc)
     * @see org.apache.slider.server.appmaster.web.view.ContainerStatsBlock$TableContent#printCell()
     */
    @Override
    public void printCell(TR<?> tableRow) {
      tableRow.td().a(anchorUrl, getCell())._();
    }
  }
}
