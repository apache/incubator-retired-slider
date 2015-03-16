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

package org.apache.slider.server.appmaster.state;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Node map map -and methods to work with it. 
 * Not Synchronized: caller is expected to lock access.
 */
public class NodeMap extends HashMap<String, NodeInstance> {
  protected static final Logger log =
    LoggerFactory.getLogger(NodeMap.class);

  /**
   * number of roles
   */
  private final int roleSize;

  /**
   * Construct
   * @param roleSize number of roles
   */
  public NodeMap(int roleSize) {
    this.roleSize = roleSize;
  }

  /**
   * Get the node instance for the specific node -creating it if needed
   * @param hostname node
   * @return the instance
   */
  public NodeInstance getOrCreate(String hostname) {
    NodeInstance node = get(hostname);
    if (node == null) {
      node = new NodeInstance(hostname, roleSize);
      put(hostname, node);
    }
    return node;
  }

  /**
   * List the active nodes
   * @param role role
   * @return a possibly empty sorted list of all nodes that are active
   * in that role
   */
  public List<NodeInstance> listActiveNodes(int role) {
    List<NodeInstance> nodes = new ArrayList<>();
    for (NodeInstance instance : values()) {
      if (instance.getActiveRoleInstances(role) > 0) {
        nodes.add(instance);
      }
    }
    Collections.sort(nodes, new NodeInstance.MoreActiveThan(role));
    return nodes;
  }

  /**
   * purge the history of all nodes that have been inactive since the absolute time
   * @param absoluteTime time
   * @return the number purged
   */
  public int purgeUnusedEntries(long absoluteTime) {
    int purged = 0;
    Iterator<Map.Entry<String, NodeInstance>> iterator =
      entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, NodeInstance> entry = iterator.next();
      NodeInstance ni = entry.getValue();
      if (!ni.purgeUnusedEntries(absoluteTime)) {
        iterator.remove();
        purged ++;
      }
    }
    return purged;
  }
  
  
  /**
   * reset the failed recently counters
   */
  public void resetFailedRecently() {
    for (Map.Entry<String, NodeInstance> entry : entrySet()) {
      NodeInstance ni = entry.getValue();
      ni.resetFailedRecently();
    }
  }

  /**
   * Clone point
   * @return
   */
  @Override
  public Object clone() {
    return super.clone();
  }

  /**
   * Insert a list of nodes into the map; overwrite any with that name
   * This is a bulk operation for testing.
   * @param nodes collection of nodes.
   */
  @VisibleForTesting
  public void insert(Collection<NodeInstance> nodes) {
    for (NodeInstance node : nodes) {
      put(node.hostname, node);
    }
  }
}
