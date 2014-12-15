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

package org.apache.slider.server.appmaster.web.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.http.FilterContainer;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Offline filter. 
 * All filter instances share the static offline flag.
 * <p>
 *   Callers must use the method {@link #bindFilter(FilterContainer)}
 *   to bind to the web container.
 */
public class OfflineFilter implements Filter {

  private static final AtomicBoolean offline = new AtomicBoolean(true);

  private static String offlineMessage = "offline";
  private static final AtomicInteger retry = new AtomicInteger(10);

  public OfflineFilter() {
  }

  public static synchronized void goOffline(String message) {
    Preconditions.checkArgument(message != null, "null message");
      offline.set(true);
      offlineMessage = message;
  }

  public static synchronized String getOfflineMessage() {
    return offlineMessage;
  }

  public static int getRetry() {
    return retry.intValue();
  }

  public static void setRetry(int retryCount) {
    retry.set(retryCount);
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest request,
      ServletResponse response,
      FilterChain chain) throws IOException, ServletException {

    if (!offline.get()) {
      chain.doFilter(request, response);
    } else {
      // service is offline
      HttpServletResponse httpResponse = (HttpServletResponse) response;
      httpResponse.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
          getOfflineMessage());
    }
  }

  @Override
  public void destroy() {

  }

  /**
   * Add the filter to a container
   * @param container container
   */
  public static void bindFilter(FilterContainer container) {
    container.addFilter("OfflineFilter",
        "org.apache.slider.server.appmaster.web.utils.OfflineFilter",
        null);
  }
}
