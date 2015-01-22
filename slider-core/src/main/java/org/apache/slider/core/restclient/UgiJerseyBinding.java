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
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathAccessDeniedException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Class to bond to a Jersey client, for UGI integration and SPNEGO.
 * <p>
 *   Usage: create an instance, then when creating a Jersey <code>Client</code>
 *   pass in to the constructor the handler provided by {@link #getHandler()}
 *
 * see <a href="https://jersey.java.net/apidocs/1.17/jersey/com/sun/jersey/client/urlconnection/HttpURLConnectionFactory.html">Jersey docs</a>
 */
public class UgiJerseyBinding implements
    HttpURLConnectionFactory {
  private static final Logger log =
      LoggerFactory.getLogger(UgiJerseyBinding.class);
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
   * Create an instance off the configuration. The SPNEGO policy
   * is derived from the current UGI settings.
   * @param conf config
   */
  public UgiJerseyBinding(Configuration conf) {
    this(new UrlConnectionOperations(conf));
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
  
  /**
   * Get the SPNEGO flag (as found in the operations instance
   * @return the spnego policy
   */
  public boolean isUseSpnego() {
    return operations.isUseSpnego();
  }


  /**
   * Uprate error codes 400 and up into faults; 
   * 404 is converted to a {@link NotFoundException},
   * 401 to {@link ForbiddenException}
   *
   * @param verb HTTP Verb used
   * @param url URL as string
   * @param ex exception
   * @throws PathNotFoundException for an unknown resource
   * @throws PathAccessDeniedException for access denied
   * @throws PathIOException for anything else
   */
  public static IOException uprateFaults(HttpVerb verb, String url,
      UniformInterfaceException ex)
      throws IOException {

    ClientResponse response = ex.getResponse();
    int resultCode = response.getStatus();
    String msg = verb.toString() + " " + url;
    if (resultCode == 404) {
      return (IOException) new PathNotFoundException(url).initCause(ex);
    }
    if (resultCode == 401) {
      return (IOException) new PathAccessDeniedException(url).initCause(ex);
    }
    // all other error codes

    
    // get a string respnse
    String message = msg +
                     " failed with exit code " + resultCode
                     + ", message " + ex.toString();
    log.error(message, ex);
    return (IOException) new PathIOException(url, message).initCause(ex);
  }
}


