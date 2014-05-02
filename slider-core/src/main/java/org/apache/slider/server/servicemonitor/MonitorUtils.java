/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.server.servicemonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Various utils to work with the monitor
 */
public final class MonitorUtils {
  protected static final Logger log = LoggerFactory.getLogger(MonitorUtils.class);

  private MonitorUtils() {
  }

  public static String toPlural(int val) {
    return val != 1 ? "s" : "";
  }

  /**
   * Convert the arguments -including dropping any empty strings that creep in
   * @param args arguments
   * @return a list view with no empty strings
   */
  public static List<String> prepareArgs(String[] args) {
    List<String> argsList = new ArrayList<String>(args.length);
    StringBuilder argsStr = new StringBuilder("Arguments: [");
    for (String arg : args) {
      argsStr.append('"').append(arg).append("\" ");
      if (!arg.isEmpty()) {
        argsList.add(arg);
      }
    }
    argsStr.append(']');
    log.debug(argsStr.toString());
    return argsList;
  }

  /**
   * convert the system properties to a buffer for printing
   * @return a multi-line list of key-value pairs
   */
  public static String dumpSystemProperties() {
    TreeSet<String> keys = new TreeSet<String>();
    for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
      keys.add(entry.getKey().toString());
    }
    StringBuilder builder = new StringBuilder();
    for (String key : keys) {
      builder
        .append("  ")
        .append(key)
        .append('=')
        .append(System.getProperty(key))
        .append('\n');
    }
    return builder.toString();
  }

  /**
   * Dump the system environment
   * @return the environment
   */
  public static String dumpEnv() {
    Map<String, String> env = System.getenv();
    TreeSet<String> keys = new TreeSet<String>();
    for (Map.Entry<String, String> entry : env.entrySet()) {
      keys.add(entry.getKey());
    }
    StringBuilder builder = new StringBuilder();
    for (String key : keys) {
      builder
        .append("  ")
        .append(key)
        .append('=')
        .append(env.get(key))
        .append('\n');
    }
    return builder.toString();
  }

  /**
   * Convert milliseconds to human time -the exact format is unspecified
   * @param milliseconds a time in milliseconds
   * @return a time that is converted to human intervals
   */
  public static String millisToHumanTime(long milliseconds) {
    StringBuilder sb = new StringBuilder();
    // Send all output to the Appendable object sb
    Formatter formatter = new Formatter(sb, Locale.US);

    long s = Math.abs(milliseconds / 1000);
    long m = Math.abs(milliseconds % 1000);
    if (milliseconds > 0) {
      formatter.format("%d.%03ds", s, m);
    } else if (milliseconds == 0) {
      formatter.format("0");
    } else {
      formatter.format("-%d.%03ds", s, m);
    }
    return sb.toString();
  }

  public static InetSocketAddress getURIAddress(URI uri) {
    String host = uri.getHost();
    int port = uri.getPort();
    return new InetSocketAddress(host, port);
  }


  /**
   * Get the localhost -may be null
   * @return the localhost if known
   */
  public static InetAddress getLocalHost() {
    InetAddress localHost;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      localHost = null;
    }
    return localHost;
  }

}
