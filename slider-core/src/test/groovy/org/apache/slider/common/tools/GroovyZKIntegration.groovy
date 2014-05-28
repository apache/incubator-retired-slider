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

package org.apache.slider.common.tools

import org.apache.slider.core.zk.ZKCallback
import org.apache.zookeeper.WatchedEvent

class GroovyZKIntegration {

/**
 * Create a ZK watcher callback that forwards the event to the
 * specific closure
 * @param closure closure to invoke
 * @return a callback which can be registered
 */
  static ZKCallback watcher(Closure closure) {
    return new ZKCallback() {
      @Override
      void process(WatchedEvent event) {
        closure(event);
      }
    }


  }
}
