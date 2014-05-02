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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

/**
 * A node instance -stores information about a node in the cluster.
 * 
 * Operations on the array/set of roles are synchronized.
 */
public class NodeInstance {

  public final String hostname;

  private final List<NodeEntry> nodeEntries;

  /**
   * Create an instance and the (empty) array of nodes
   * @param roles role count -the no. of roles
   */
  public NodeInstance(String hostname, int roles) {
    this.hostname = hostname;
    nodeEntries = new ArrayList<NodeEntry>(roles);
  }

  /**
   * Get the entry for a role -if present
   * @param role role index
   * @return the entry
   * null if the role is out of range
   */
  public synchronized NodeEntry get(int role) {
    for (NodeEntry nodeEntry : nodeEntries) {
      if (nodeEntry.index == role) {
        return nodeEntry;
      }
    }
    return null;
  }
  
  /**
   * Get the entry for a role -if present
   * @param role role index
   * @return the entry
   * @throws ArrayIndexOutOfBoundsException if the role is out of range
   */
  public synchronized NodeEntry getOrCreate(int role) {
    NodeEntry entry = get(role);
    if (entry == null) {
      entry = new NodeEntry(role);
      nodeEntries.add(entry);
    }
    return entry;
  }

  /**
   * Cout the number of active role instances on this node
   * @param role role index
   * @return 0 if there are none, otherwise the #of nodes that are running and
   * not being released already.
   */
  public int getActiveRoleInstances(int role) {
    NodeEntry nodeEntry = get(role);
    return (nodeEntry != null ) ? nodeEntry.getActive() : 0;
  }

  /**
   * Get the entry for a role -and remove it if present
   * @param role the role index
   * @return the entry that WAS there
   */
  public synchronized NodeEntry remove(int role) {
    NodeEntry nodeEntry = get(role);
    if (nodeEntry != null) {
      nodeEntries.remove(nodeEntry);
    }
    return nodeEntry;
  }

  public synchronized void set(int role, NodeEntry nodeEntry) {
    remove(role);
    nodeEntries.add(nodeEntry);
  }

  /**
   * run through each entry; gc'ing & removing old ones
   * @param absoluteTime age in millis
   * @return true if there are still entries left
   */
  public synchronized boolean purgeUnusedEntries(long absoluteTime) {
    boolean active = false;
    ListIterator<NodeEntry> entries = nodeEntries.listIterator();
    while (entries.hasNext()) {
      NodeEntry entry = entries.next();
      if (entry.notUsedSince(absoluteTime)) {
        entries.remove();
      } else {
        active = true;
      }
    }
    return active;
  }

  @Override
  public String toString() {
    return hostname;
  }

  /**
   * Full dump of entry including children
   * @return a multi-line description fo the node
   */
  public String toFullString() {
    final StringBuilder sb =
      new StringBuilder(toString());
    int i = 0;
    for (NodeEntry entry : nodeEntries) {
      sb.append(String.format("\n  [%02d]  ", i++));
        sb.append(entry.toString());
    }
    return sb.toString();
  }

  /**
   * Equality test is purely on the hostname of the node address
   * @param o other
   * @return true if the hostnames are equal
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeInstance that = (NodeInstance) o;
    return hostname.equals(that.hostname);
  }

  @Override
  public int hashCode() {
    return hostname.hashCode();
  }

  /**
   * A comparator for sorting entries where the role is newer than
   * the other. 
   * This sort only compares the lastUsed field, not whether the
   * node is in use or not
   */
  public static class newerThan implements Comparator<NodeInstance>,
                                           Serializable {

    final int role;

    public newerThan(int role) {
      this.role = role;
    }

    @Override
    public int compare(NodeInstance o1, NodeInstance o2) {
      long age = o1.getOrCreate(role).getLastUsed();
      long age2 = o2.getOrCreate(role).getLastUsed();
      
      if (age > age2) {
        return -1;
      } else if (age < age2) {
        return 1;
      }
      // equal
      return 0;
    }
  }
  
  /**
   * A comparator for sorting entries where the role is newer than
   * the other. 
   * This sort only compares the lastUsed field, not whether the
   * node is in use or not
   */
  public static class moreActiveThan implements Comparator<NodeInstance>,
                                           Serializable {

    final int role;

    public moreActiveThan(int role) {
      this.role = role;
    }

    @Override
    public int compare(NodeInstance left, NodeInstance right) {
      int activeLeft = left.getActiveRoleInstances(role);
      int activeRight = right.getActiveRoleInstances(role);
      return activeRight - activeLeft;
    }
  }

}
