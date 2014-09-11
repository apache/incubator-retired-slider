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

package org.apache.chaos.remote

import org.apache.hadoop.conf.Configured

/**
 *
 */
class BuildPidPath extends Configured {

  public static final String PID_DIR = "/var/run/hadoop"


  String buildPidName(String user, String service) {
    "${user}-${service}"
  }

  File buildPidPath(String pidName) {
    new File(PID_DIR, "hadoop-${pidName}.pid")
  }

  /**
   * Find the Pid string value of a hadoop process
   * @param user user ID
   * @param service
   * @return the pid as a string or null for none found. If the string
   * is empty it means the pid file was empty -there's no validation of
   * it.
   */
  String findPid(String pidName) {
    File pidFile = buildPidPath(pidName)
    if (!pidFile.exists()) {
      return null
    } else {
      return pidFile.text.trim()
    }
  }
}
