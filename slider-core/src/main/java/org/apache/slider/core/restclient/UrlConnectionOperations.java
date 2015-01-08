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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Operations on the JDK UrlConnection class. This uses WebHDFS
 * methods to set up the operations.
 */
public class UrlConnectionOperations extends Configured {
  private static final Logger log =
      LoggerFactory.getLogger(UrlConnectionOperations.class);

  private URLConnectionFactory connectionFactory;

  public UrlConnectionOperations(Configuration conf) {
    super(conf);
    connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(conf);  }

  /**
   * Opens a url with read and connect timeouts
   *
   * @param url
   *          to open
   * @return URLConnection
   * @throws IOException
   */
  public HttpURLConnection openConnection(URL url, boolean spnego) throws
      IOException,
      AuthenticationException {
    Preconditions.checkArgument(url.getPort() != 0, "no port");
    HttpURLConnection conn =
        (HttpURLConnection) connectionFactory.openConnection(url, spnego);
    conn.setUseCaches(false);
    conn.setInstanceFollowRedirects(true);
    return conn;
  }

  public byte[] execGet(URL url, boolean spnego) throws
      IOException,
      AuthenticationException {
    HttpURLConnection conn = null;
    int resultCode;
    byte[] body = null;
    log.debug("GET {} spnego={}", url, spnego);

    try {
      conn = openConnection(url, spnego);
      resultCode = conn.getResponseCode();
      InputStream stream = conn.getErrorStream();
      if (stream == null) {
        stream = conn.getInputStream();
      }
      if (stream != null) {
        // read into a buffer.
        body = IOUtils.toByteArray(stream);
      } else {
        // no body: 
        log.debug("No body in response");

      }
    } catch (IOException e) {
      throw NetUtils.wrapException(url.toString(), 
          url.getPort(), "localhost", 0, e);

    } catch (AuthenticationException e) {
      throw new IOException("From " + url + ": " + e.toString(), e);

    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    uprateFaults(url.toString(), resultCode, body);
    return body;
  }

  /**
   * Uprate error codes 400 and up into faults; 
   * 404 is converted to a {@link NotFoundException},
   * 401 to {@link ForbiddenException}
   * @param url URL as string
   * @param resultCode response from the request
   * @param body optional body of the request
   * @throws IOException if the result was considered a failure
   */
  public static void uprateFaults(String url,
      int resultCode, byte[] body)
      throws IOException {

    if (resultCode < 400) {
      //success
      return;
    }
    if (resultCode == 404) {
      throw new NotFoundException(url);
    }
    if (resultCode == 401) {
      throw new ForbiddenException(url);
    }
    // all other error codes
    String bodyAsString;
    if (body != null && body.length > 0) {
      bodyAsString = new String(body);
    } else {
      bodyAsString = "";
    }
    String message = "Request to " + url +
                     " failed with exit code " + resultCode
                     + ", body length " + bodyAsString.length()
                     + ":\n" + bodyAsString;
    log.error(message);
    throw new IOException(message);
  }

}
