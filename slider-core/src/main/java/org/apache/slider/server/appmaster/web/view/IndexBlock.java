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
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 
 */
public class IndexBlock extends HtmlBlock {
  private static final Logger log = LoggerFactory.getLogger(IndexBlock.class);

  private StateAccessForProviders appView;
  private ProviderService providerService;

  @Inject
  public IndexBlock(WebAppApi slider) {
    this.appView = slider.getAppState();
    this.providerService = slider.getProviderService();
  }

  @Override
  protected void render(Block html) {
    final String providerName = getProviderName();

    doIndex(html, providerName);
  }

  // An extra method to make testing easier since you can't make an instance of Block
  @VisibleForTesting
  protected void doIndex(Hamlet html, String providerName) {
    ClusterDescription clusterStatus = appView.getClusterStatus();
    DIV<Hamlet> div = html.div("general_info")
                          .h1("index_header",
                              "Application: '" + clusterStatus.name + "'");

    UL<DIV<Hamlet>> ul = div.ul();

    ul.li("Total number of containers for application: " + appView.getNumOwnedContainers());
    ul.li("Application created: " +
          getInfoAvoidingNulls(StatusKeys.INFO_CREATE_TIME_HUMAN));
    ul.li("Application last flexed: " + getInfoAvoidingNulls(StatusKeys.INFO_FLEX_TIME_HUMAN));
    ul.li("Application running since: " + getInfoAvoidingNulls(StatusKeys.INFO_LIVE_TIME_HUMAN));
    ul.li("Application HDFS storage path: " + clusterStatus.dataPath);
    ul.li("Application configuration path: " + clusterStatus.originConfigurationPath);

    ul._()._();

    html.div("provider_info").h3(providerName + " specific information");
    ul = div.ul();
    addProviderServiceOptions(providerService, ul, clusterStatus);
    ul._()._();

    html.div("container_instances").h3("Component Instances");

    Hamlet.TABLE<DIV<Hamlet>> table = div.table();
    table.tr()
         .th("Component")
         .th("Desired")
         .th("Actual")
         .th("Requested")
         .th("Failed")
         .th("Failed to start")
         ._();

    List<RoleStatus> roleStatuses = appView.cloneRoleStatusList();
    Collections.sort(roleStatuses, new RoleStatus.CompareByName());
    for (RoleStatus status : roleStatuses) {
      table.tr()
           .td(status.getName())
           .th(String.format("%d", status.getDesired()))
           .th(String.format("%d", status.getActual()))
           .th(String.format("%d", status.getRequested()))
           .th(String.format("%d", status.getFailed()))
           .th(String.format("%d", status.getStartFailed()))
            ._();
    }

    table._()._();
  }

  private String getProviderName() {
    String providerServiceName = providerService.getName().toLowerCase(Locale.ENGLISH);
    return providerServiceName;
  }

  private String getInfoAvoidingNulls(String key) {
    String createTime = appView.getClusterStatus().getInfo(key);

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
