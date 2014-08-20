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

package org.apache.slider.server.appmaster.web;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import java.util.HashMap;
import java.util.Map;

public class SliderAmFilterInitializer extends FilterInitializer {
  private static final String FILTER_NAME = "AM_PROXY_FILTER";
  private static final String FILTER_CLASS = SliderAmIpFilter.class.getCanonicalName();
  private static final String HTTPS_PREFIX = "https://";
  private static final String HTTP_PREFIX = "http://";
  private Configuration configuration;

  public static final String NAME =
    "org.apache.slider.server.appmaster.web.SliderAmFilterInitializer";

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {
    configuration = conf;
    Map<String, String> params = new HashMap<String, String>();
    String proxy = WebAppUtils.getProxyHostAndPort(conf);
    String[] parts = proxy.split(":");
    params.put(SliderAmIpFilter.PROXY_HOST, parts[0]);
    // todo:  eventually call WebAppUtils.getHttpSchemePrefix
    params.put(SliderAmIpFilter.PROXY_URI_BASE, getHttpSchemePrefix()
        + proxy + getApplicationWebProxyBase());
    params.put(SliderAmIpFilter.WS_CONTEXT_ROOT,
               conf.get(SliderAmIpFilter.WS_CONTEXT_ROOT));
    container.addFilter(FILTER_NAME, FILTER_CLASS, params);
  }

  @VisibleForTesting
  protected String getApplicationWebProxyBase() {
    return System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV);
  }

  private String getHttpSchemePrefix() {
    return HttpConfig.Policy.HTTPS_ONLY ==
           HttpConfig.Policy.fromString(configuration
                                          .get(
                                            YarnConfiguration.YARN_HTTP_POLICY_KEY,
                                            YarnConfiguration.YARN_HTTP_POLICY_DEFAULT))
           ? HTTPS_PREFIX : HTTP_PREFIX;
  }
}
