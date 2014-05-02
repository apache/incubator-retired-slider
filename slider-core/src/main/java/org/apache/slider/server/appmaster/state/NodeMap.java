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
    List<NodeInstance> nodes = new ArrayList<NodeInstance>();
    for (NodeInstance instance : values()) {
      if (instance.getActiveRoleInstances(role) > 0) {
        nodes.add(instance);
      }
    }
    Collections.sort(nodes, new NodeInstance.moreActiveThan(role));
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
   * Find a list of node for release; algorithm may make its own
   * decisions on which to release.
   * @param role role index
   * @param count number of nodes to release
   * @return a possibly empty list of nodes.
   */
  public List<NodeInstance> findNodesForRelease(int role, int count) {
    List<NodeInstance> targets = new ArrayList<NodeInstance>(count);
    List<NodeInstance> active = listActiveNodes(role);
    List<NodeInstance> multiple = new ArrayList<NodeInstance>();
    int nodesRemaining = count;
    log.debug("searching for {} nodes with candidate set size {}",
              nodesRemaining, active.size());
    ListIterator<NodeInstance> it = active.listIterator();
    while (it.hasNext() && nodesRemaining > 0) {
      NodeInstance ni = it.next();
      int load = ni.getActiveRoleInstances(role);
      log.debug("Node {} load={}", ni, load);
      assert load != 0; 
      if (load == 1) {
        // at the tail of the list, from here active[*] is a load=1 entry
        break;
      }
      // load is >1. Add an entry to the target list FOR EACH INSTANCE ABOVE 1
      for (int i = 0; i < (load - 1) && nodesRemaining > 0; i++) {
        nodesRemaining--;
        log.debug("Push {} #{}", ni, i);
        targets.add(ni);
      }
      // and add to the multiple list
      multiple.add(ni);
      // then pop it from the active list
      it.remove();
    }
    //here either the number is found or there is still some left.

    if (nodesRemaining > 0) {
      // leftovers. Append any of the multiple node entries to the tail of 
      // the active list (so they get chosen last)
      active.addAll(multiple);
      // all the entries in the list have exactly one node
      // so ask for as many as are needed
      int ask = Math.min(nodesRemaining, active.size());
      log.debug("load=1 nodes to select={} multiples={} available={} ask={}",
                nodesRemaining, multiple.size(),active.size(), ask);
      targets.addAll(active.subList(0, ask));
    }
    return targets;
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
