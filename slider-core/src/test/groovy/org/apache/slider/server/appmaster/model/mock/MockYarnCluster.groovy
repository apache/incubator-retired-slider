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

package org.apache.slider.server.appmaster.model.mock

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.NodeId

/**
 * Models the cluster itself: a set of mock cluster nodes.
 *
 * nodes retain the slot model with a limit of 2^8 slots/host -this
 * lets us use 24 bits of the container ID for hosts, and so simulate
 * larger hosts.
 *
 * upper 32: index into nodes in the cluster
 * NodeID hostname is the index in hex format; this is parsed down to the index
 * to resolve the host
 *
 * Important: container IDs will be reused as containers get recycled. This
 * is not an attempt to realistically mimic a real YARN cluster, just 
 * simulate it enough for Slider to explore node re-use and its handling
 * of successful and unsuccessful allocations.
 *
 * There is little or no checking of valid parameters in here -this is for
 * test use, not production.
 */
@CompileStatic
@Slf4j
public class MockYarnCluster {

  final int clusterSize;
  final int containersPerNode;
  MockYarnClusterNode[] nodes;

  MockYarnCluster(int clusterSize, int containersPerNode) {
    this.clusterSize = clusterSize
    this.containersPerNode = containersPerNode
    build();
  }

  @Override
  String toString() {
    return "MockYarnCluster size=$clusterSize, capacity=${totalClusterCapacity()},"+
        " in use=${containersInUse()}"
  }

  /**
   * Build the cluster.
   */
  private void build() {
    nodes = new MockYarnClusterNode[clusterSize]
    for (int i = 0; i < clusterSize; i++) {
      nodes[i] = new MockYarnClusterNode(i, containersPerNode)
    }
  }

  MockYarnClusterNode nodeAt(int index) {
    return nodes[index]
  }

  MockYarnClusterNode lookup(String hostname) {
    int index = Integer.valueOf(hostname, 16)
    return nodeAt(index)
  }

  MockYarnClusterNode lookup(NodeId nodeId) {
    return lookup(nodeId.host)
  }

  MockYarnClusterNode lookupOwner(ContainerId cid) {
    return nodeAt(extractHost(cid.id))
  }

  /**
   * Release a container: return true if it was actually in use
   * @param cid container ID
   * @return the container released
   */
  MockYarnClusterContainer release(ContainerId cid) {
    int host = extractHost(cid.id)
    def inUse = nodeAt(host).release(cid.id)
    log.debug("Released $cid inuse=$inUse")
    return inUse
  }

  int containersInUse() {
    int count = 0;
    nodes.each { MockYarnClusterNode it -> count += it.containersInUse() }
    return count;
  }

  /**
   * Containers free
   * @return
   */
  int containersFree() {
    return totalClusterCapacity() - containersInUse();
  }

  int totalClusterCapacity() {
    return clusterSize * containersPerNode;
  }

  /**
   * Reset all the containers
   */
  public void reset() {
    nodes.each { MockYarnClusterNode node ->
      node.reset()
    }
  }

  /**
   * Bulk allocate the specific number of containers on a range of the cluster
   * @param startNode start of the range
   * @param endNode end of the range
   * @param count count 
   * @return the number actually allocated -it will be less the count supplied
   * if the node was full
   */
  public int bulkAllocate(int startNode, int endNode, int count) {
    int total = 0;
    for (int i in startNode..endNode) {
      total += nodeAt(i).bulkAllocate(count).size()
    }
    return total;
  }
  
  
/**
 * Model cluster nodes on the simpler "slot" model than the YARN-era
 * resource allocation model. Why? Makes it easier to implement.
 *
 * When a cluster is offline, 
 */
  public static class MockYarnClusterNode {

    public final int nodeIndex
    public final String hostname;
    public final MockNodeId nodeId;
    public final MockYarnClusterContainer[] containers;
    private boolean offline;

    public MockYarnClusterNode(int index, int size) {
      nodeIndex = index;
      hostname = String.format(Locale.ENGLISH, "%08x", index)
      nodeId = new MockNodeId(hostname, 0);

      containers = new MockYarnClusterContainer[size];
      for (int i = 0; i < size; i++) {
        int cid = makeCid(index, i);
        MockContainerId mci = new MockContainerId(containerId: cid)
        containers[i] = new MockYarnClusterContainer(mci)
      }
    }

    /**
     * Look up a container
     * @param containerId
     * @return
     */
    public MockYarnClusterContainer lookup(int containerId) {
      return containers[extractContainer(containerId)]
    }

    /**
     * Go offline; release all containers
     */
    public void goOffline() {
      if (!offline) {
        offline = true;
        reset()
      }
    }
    
    public void goOnline() {
      offline = false;
    }

    /**
     * allocate a container -if one is available 
     * @return the container or null for none free
     * -or the cluster node is offline
     */
    public MockYarnClusterContainer allocate() {
      if (!offline) {
        for (int i = 0; i < containers.size(); i++) {
          MockYarnClusterContainer c = containers[i]
          if (!c.busy) {
            c.busy = true;
            return c;
          }
        }
      }
      return null;
    }

    /**
     * Bulk allocate the specific number of containers
     * @param count count
     * @return the list actually allocated -it will be less the count supplied
     * if the node was full
     */
    public List<MockYarnClusterContainer> bulkAllocate(int count) {
      List < MockYarnClusterContainer > result = []
      for (int i = 0; i < count; i++) {
        MockYarnClusterContainer allocation = allocate();
        if (allocation == null) {
          break;
        }
        result << allocation
      }
      return result
    }
    
    

    /**
     * Release a container
     * @param cid container ID
     * @return the container if the container was busy before the release
     */
    public MockYarnClusterContainer release(int cid) {
      MockYarnClusterContainer container = containers[extractContainer(cid)]
      boolean b = container.busy;
      container.busy = false;
      return b? container: null;
    }

    public String httpAddress() {
      return "http://$hostname/"
    }

    /**
     * Reset all the containers
     */
    public void reset() {
      containers.each { MockYarnClusterContainer cont ->
        cont.reset()
      }
    }
    
   public int containersInUse() {
      int c = 0;
      containers.each { MockYarnClusterContainer cont ->
        c += cont.busy ? 1 : 0
      }
      return c
    }

    public int containersFree() {
      return containers.length - containersInUse();
    }
  }

  /**
   * Cluster container
   */
  public static class MockYarnClusterContainer {
    MockContainerId cid;
    boolean busy;

    MockYarnClusterContainer(MockContainerId cid) {
      this.cid = cid
    }
    
    void reset() {
      busy = false;
    }
  }

  public static int makeCid(int hostIndex, int containerIndex) {
    return (hostIndex << 8) | containerIndex & 0xff;
  }

  public static final int extractHost(int cid) {
    return (cid >>> 8);
  }

  public static final int extractContainer(int cid) {
    return (cid & 0xff);
  }

}
