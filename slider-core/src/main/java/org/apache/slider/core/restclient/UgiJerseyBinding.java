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

package org.apache.slider.core.restclient;

import com.google.common.base.Preconditions;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Class to bond to a Jersey client, for UGI integration and SPNEGO.
 * <p>
 *   Usage: create an instance, then when creating a Jersey <code>Client</code>
 *   pass in to the constructor the handler provided by {@link #getHandler()}
 *
 * @see https://jersey.java.net/apidocs/1.17/jersey/com/sun/jersey/client/urlconnection/HttpURLConnectionFactory.html
 */
public class UgiJerseyBinding implements
    HttpURLConnectionFactory {
  private final UrlConnectionOperations operations;
  private final URLConnectionClientHandler handler;

  /**
   * Construct an instance
   * @param operations operations instance
   */
  @SuppressWarnings("ThisEscapedInObjectConstruction")
  public UgiJerseyBinding(UrlConnectionOperations operations) {
    Preconditions.checkArgument(operations != null, "Null operations");
    this.operations = operations;
    handler = new URLConnectionClientHandler(this);
  }

  /**
   * Get a URL connection. 
   * @param url
   * @return the connection
   * @throws IOException any problem. {@link AuthenticationException} 
   * errors are wrapped
   */
  @Override
  public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
    try {
      return operations.openConnection(url);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }
  }

  public UrlConnectionOperations getOperations() {
    return operations;
  }

  public URLConnectionClientHandler getHandler() {
    return handler;
  }
}


