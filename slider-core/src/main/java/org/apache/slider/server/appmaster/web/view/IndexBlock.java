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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.UL;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.RoleStatistics;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_COMPONENTS;

/**
 * 
 */
public class IndexBlock extends SliderHamletBlock {
  private static final Logger log = LoggerFactory.getLogger(IndexBlock.class);

  /**
   * Message printed when application is at full size.
   *
   * {@value}
   */
  public static final String ALL_CONTAINERS_ALLOCATED = "all containers allocated";

  @Inject
  public IndexBlock(WebAppApi slider) {
    super(slider);
  }

  @Override
  protected void render(Block html) {
    final String providerName = getProviderName();

    doIndex(html, providerName);
  }

  // An extra method to make testing easier since you can't make an instance of Block
  @VisibleForTesting
  protected void doIndex(Hamlet html, String providerName) {
    ClusterDescription clusterStatus = appState.getClusterStatus();
    RoleStatistics roleStats = appState.getRoleStatistics();
    String name = clusterStatus.name;
    if (name != null && (name.startsWith(" ") || name.endsWith(" "))) {
      name = "'" + name + "'";
    } 
    DIV<Hamlet> div = html.div("general_info")
                          .h1("index_header",
                              "Application: " + name);

    ApplicationLivenessInformation liveness =
        appState.getApplicationLivenessInformation();
    String livestatus = liveness.allRequestsSatisfied
        ? ALL_CONTAINERS_ALLOCATED
        : String.format("Awaiting %d containers", liveness.requestsOutstanding);
    Hamlet.TABLE<DIV<Hamlet>> table1 = div.table();
    table1.tr()
          .td("Status")
          .td(livestatus)
          ._();
    table1.tr()
          .td("Total number of containers")
          .td(Integer.toString(appState.getNumOwnedContainers()))
          ._();
    table1.tr()
          .td("Create time: ")
          .td(getInfoAvoidingNulls(StatusKeys.INFO_CREATE_TIME_HUMAN))
          ._();
    table1.tr()
          .td("Running since: ")
          .td(getInfoAvoidingNulls(StatusKeys.INFO_LIVE_TIME_HUMAN))
          ._();
    table1.tr()
          .td("Time last flexed: ")
          .td(getInfoAvoidingNulls(StatusKeys.INFO_FLEX_TIME_HUMAN))
          ._();
    table1.tr()
          .td("Application storage path: ")
          .td(clusterStatus.dataPath)
          ._();
    table1.tr()
          .td("Application configuration path: ")
          .td(clusterStatus.originConfigurationPath)
          ._();
    table1._()._();


    html.div("container_instances").h3("Component Instances");

    Hamlet.TABLE<DIV<Hamlet>> table = div.table();
    Hamlet.TR<Hamlet.THEAD<Hamlet.TABLE<DIV<Hamlet>>>> tr = table.thead().tr();
    trb(tr, "Component");
    trb(tr, "Desired");
    trb(tr, "Actual");
    trb(tr, "Outstanding Requests");
    trb(tr, "Failed");
    trb(tr, "Failed to start");
    trb(tr, "Placement");
    tr._()._();

    List<RoleStatus> roleStatuses = appState.cloneRoleStatusList();
    Collections.sort(roleStatuses, new RoleStatus.CompareByName());
    for (RoleStatus status : roleStatuses) {
      String roleName = status.getName();
      String nameUrl = apiPath(LIVE_COMPONENTS) + "/" + roleName;
      String aatext;
      if (status.isAntiAffinePlacement()) {
        int outstanding = status.isAARequestOutstanding() ? 1: 0;
        int pending = (int)status.getPendingAntiAffineRequests();
        aatext = String.format("Anti-affine: %d outstanding %s, %d pending %s",
          outstanding, plural(outstanding, "request"),
          pending, plural(pending, "request"));
      } else {
        aatext = "";
      }
      table.tr()
        .td().a(nameUrl, roleName)._()
        .td(String.format("%d", status.getDesired()))
        .td(String.format("%d", status.getActual()))
        .td(String.format("%d", status.getRequested()))
        .td(String.format("%d", status.getFailed()))
        .td(String.format("%d", status.getStartFailed()))
        .td(aatext)
        ._();
    }

    table._()._();

    // some spacing
    html.p()._();
    html.p()._();

    html.div("provider_info").h3(providerName + " information");
    UL<DIV<Hamlet>> ul = div.ul();
    addProviderServiceOptions(providerService, ul, clusterStatus);
    ul._()._();
  }

  private String plural(int n, String text) {
    return n == 1 ? text : (text + "s");
  }

  private void trb(Hamlet.TR<Hamlet.THEAD<Hamlet.TABLE<DIV<Hamlet>>>> tr,
      String text) {
    tr.td().b(text)._();
  }

  private String getProviderName() {
    return providerService.getHumanName();
  }

  private String getInfoAvoidingNulls(String key) {
    String createTime = appState.getClusterStatus().getInfo(key);

    return null == createTime ? "N/A" : createTime;
  }

  protected void addProviderServiceOptions(ProviderService providerService,
      UL<DIV<Hamlet>> ul, ClusterDescription clusterStatus) {
    Map<String, String> details = providerService.buildMonitorDetails(
        clusterStatus);
    if (null == details) {
      return;
    }
    // Loop over each entry, placing the text in the UL, adding an anchor when the URL is non-null/empty
    for (Entry<String, String> entry : details.entrySet()) {
      String url = entry.getValue();
      if (SliderUtils.isSet(url) ) {
        ul.li()._(entry.getKey()).a(url, url)._();
      } else {
        ul.li(entry.getKey());
      }
    }
  }


}
