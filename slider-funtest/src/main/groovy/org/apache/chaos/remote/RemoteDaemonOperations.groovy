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

/**
 * Operations to perform on a remote init.d service by ssh-ing in and issuing
 * the command as root. 
 * Use the full name, such as "hadoop-namenode"
 */
class RemoteDaemonOperations {

  public static final int TIMEOUT = 0
  RemoteServer server
  String name;

  RemoteDaemonOperations(RemoteServer server, String name) {
    this.server = server
    this.name = name
  }

  def start() {
    exec("start")
  }

  def stop() {
    exec("stop")
  }

  def restart() {
    exec("restart")
  }

  def status() {
    exec("status")
  }

  /**
   *
   * @param action action to exec
   * @return ( rv , out )
   */
  List exec(String action) {
    server.exec([["service", name, action], "exit"])
  }
}
