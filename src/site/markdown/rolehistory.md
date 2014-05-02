<!---
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Role History: how Slider brings back nodes in the same location

### Last updated  2013-12-06

* This document uses the pre-slider terminology of role/cluster and not
component and application instance *


## Outstanding issues

1. Can we use the history to implement anti-affinity: for any role with this flag,
use our knowledge of the cluster to ask for all nodes that aren't in use already

1. How to add blacklisting here? We are tracking failures and startup failures
per node (not persisted), but not using this in role placement requests yet.

## Introduction

Slider needs to bring up instances of a given role on the machine(s) on which
they last ran -it should remember after shrinking or freezing a cluster  which
servers were last used for a role -and use this (persisted) data to select
clusters next time

It does this in the basis that the role instances prefer node-local
access to data previously persisted to HDFS. This is precisely the case
for Apache HBase, which can use Unix Domain Sockets to talk to the DataNode
without using the TCP stack. The HBase master persists to HDFS the tables
assigned to specific Region Servers, and when HBase is restarted its master
tries to reassign the same tables back to Region Servers on the same machine.

For this to work in a dynamic cluster, Slider needs to bring up Region Servers
on the previously used hosts, so that the HBase Master can re-assign the same
tables.

Note that it does not need to care about the placement of other roles, such
as the HBase masters -there anti-affinity between other instances is
the key requirement.

### Terminology

* **Role Instance** : a single instance of a role.
* **Node** : A server in the YARN Physical (or potentially virtual) Cluster of servers.
* **Slider Cluster**: The set of role instances deployed by Slider so as to 
 create a single aggregate application.
* **Slider AM**: The Application Master of Slider: the program deployed by YARN to
manage its Slider Cluster.
* **RM** YARN Resource Manager

### Assumptions

Here are some assumptions in Slider's design

1. Instances of a specific role should preferably be deployed onto different
servers. This enables Slider to only remember the set of server nodes onto
which instances were created, rather than more complex facts such as "two Region
Servers were previously running on Node #17. On restart Slider can simply request
one instance of a Region Server on a specific node, leaving the other instance
to be arbitrarily deployed by YARN. This strategy should help reduce the *affinity*
in the role deployment, so increase their resilience to failure.

1. There is no need to make sophisticated choices on which nodes to request
re-assignment -such as recording the amount of data persisted by a previous
instance and prioritizing nodes based on such data. More succinctly 'the
only priority needed when asking for nodes is *ask for the most recently used*.

1. Different roles are independent: it is not an issue if a role of one type
 (example, an Accumulo Monitor and an Accumulo Tablet Server) are on the same
 host. This assumption allows Slider to only worry about affinity issues within
 a specific role, rather than across all roles.
 
1. After a cluster has been started, the rate of change of the cluster is
low: both node failures and cluster flexing happen at the rate of every few
hours, rather than every few seconds. This allows Slider to avoid needing
data structures and layout persistence code designed for regular and repeated changes.

1. Instance placement is best-effort: if the previous placement cannot be satisfied,
the application will still perform adequately with role instances deployed
onto new servers. More specifically, if a previous server is unavailable
for hosting a role instance due to lack of capacity or availability, Slider
will not decrement the number of instances to deploy: instead it will rely
on YARN to locate a new node -ideally on the same rack.

1. If two instances of the same role do get assigned to the same server, it
is not a failure condition. (This may be problematic for some roles 
-we may need a role-by-role policy here, so that master nodes can be anti-affine)
[specifically, >1 HBase master mode will not come up on the same host]

1. If a role instance fails on a specific node, asking for a container on
that same node for the replacement instance is a valid recovery strategy.
This contains assumptions about failure modes -some randomness here may
be a valid tactic, especially for roles that do not care about locality.

1. Tracking failure statistics of nodes may be a feature to add in future;
designing the Role History datastructures to enable future collection
of rolling statistics on recent failures would be a first step to this 

### The Role History

The `RoleHistory` is a datastructure which models the role assignment, and
can persist it to and restore it from the (shared) filesystem.

* For each role, there is a list of cluster nodes which have supported this role
used in the past.

* This history is used when selecting a node for a role.

* This history remembers when nodes were allocated. These are re-requested
when thawing a cluster.

* It must also remember when nodes were released -these are re-requested
when returning the cluster size to a previous size during flex operations.

* It has to track nodes for which Slider has an outstanding container request
with YARN. This ensures that the same node is not requested more than once
due to outstanding requests.

* It does not retain a complete history of the role -and does not need to.
All it needs to retain is the recent history for every node onto which a role
instance has been deployed. Specifically, the last allocation or release
operation on a node is all that needs to be persisted.

* On AM startup, all nodes in the history are considered candidates, even those nodes currently marked
as active -as they were from the previous instance.

* On AM restart, nodes in the role history marked as active have to be considered
still active -the YARN RM will have to provide the full list of which are not.

* During cluster flexing, nodes marked as released -and for which there is no
outstanding request - are considered candidates for requesting new instances.

* When choosing a candidate node for hosting a role instance, it from the head
of the time-ordered list of nodes that last ran an instance of that role

### Persistence

The state of the role is persisted to HDFS on changes -but not on cluster
termination.

1. When nodes are allocated, the Role History is marked as dirty
1. When container release callbacks are received, the Role History is marked as dirty
1. When nodes are requested or a release request made, the Role History is *not*
 marked as dirty. This information is not relevant on AM restart.

As at startup, a large number of allocations may arrive in a short period of time,
the Role History may be updated very rapidly -yet as the containers are
only recently activated, it is not likely that an immediately restarted Slider
cluster would gain by re-requesting containers on them -their historical
value is more important than their immediate past.

Accordingly, the role history may be persisted to HDFS asynchronously, with
the dirty bit triggering an flushing of the state to HDFS. The datastructure
will still need to be synchronized for cross thread access, but the 
sync operation will not be a major deadlock, compared to saving the file on every
container allocation response (which will actually be the initial implementation).

There's no need to persist the format in a human-readable form; while protobuf
might seem the approach most consistent with the rest of YARN, it's not
an easy structure to work with.

The initial implementation will use Apache Avro as the persistence format,
with the data saved in JSON or compressed format.


## Weaknesses in this design

**Blacklisting**: even if a node fails repeatedly, this design will still try to re-request
instances on this node; there is no blacklisting. As a central blacklist
for YARN has been proposed, it is hoped that this issue will be addressed centrally,
without Slider having to remember which nodes are unreliable *for that particular
Slider cluster*.

**Anti-affinity**: If multiple role instances are assigned to the same node,
Slider has to choose on restart or flexing whether to ask for multiple
nodes on that node again, or to pick other nodes. The assumed policy is
"only ask for one node"

**Bias towards recent nodes over most-used**: re-requesting the most
recent nodes, rather than those with the most history of use, may
push Slider to requesting nodes that were only briefly in use -and so have
on a small amount of local state, over nodes that have had long-lived instances.
This is a problem that could perhaps be addressed by preserving more
history of a node -maintaining some kind of moving average of
node use and picking the heaviest used, or some other more-complex algorithm.
This may be possible, but we'd need evidence that the problem existed before
trying to address it.

# The NodeMap: the core of the Role History

The core data structure, the `NodeMap` is a map of every known node in the cluster, tracking
how many containers are allocated to specific roles in it, and, when there
are no active instances, when it was last used. This history is used to
choose where to request new containers. Because of the asynchronous
allocation and release of containers, the Role History also needs to track
outstanding release requests --and, more critically, outstanding allocation
requests. If Slider has already requested a container for a specific role
on a host, then asking for another container of that role would break
anti-affinity requirements. Note that not tracking outstanding requests would
radically simplify some aspects of the design, especially the complexity
of correlating allocation responses with the original requests -and so the
actual hosts originally requested.

1. Slider builds up a map of which nodes have recently been used.
1. Every node counts the number. of active containers in each role.
1. Nodes are only chosen for allocation requests when there are no
active or requested containers on that node.
1. When choosing which instances to release, Slider could pick the node with the
most containers on it. This would spread the load.
1. When there are no empty nodes to request containers on, a request would
let YARN choose.

#### Strengths

* Handles the multi-container on one node problem
* By storing details about every role, cross-role decisions could be possible
* Simple counters can track the state of pending add/release requests
* Scales well to a rapidly flexing cluster
* Simple to work with and persist
* Easy to view and debug
* Would support cross-role collection of node failures in future

#### Weaknesses

* Size of the data structure is `O(nodes * role-instances`). This
could be mitigated by regular cleansing of the structure. For example, at
thaw time (or intermittently) all unused nodes > 2 weeks old could be dropped.
* Locating a free node could take `O(nodes)` lookups -and if the criteria of "newest"
is included, will take exactly `O(nodes)` lookups. As an optimization, a list
of recently explicitly released nodes can be maintained.
* Need to track outstanding requests against nodes, so that if a request
was satisfied on a different node, the original node's request count is
 decremented, *not that of the node actually allocated*. 
* In a virtual cluster, may fill with node entries that are no longer in the cluster.
Slider should query the RM (or topology scripts?) to determine if nodes are still
parts of the YARN cluster. 

## Data Structures

### RoleHistory

    startTime: long
    saveTime: long
    dirty: boolean
    nodemap: NodeMap
    roles: RoleStatus[]
    outstandingRequests: transient OutstandingRequestTracker
    availableNodes: transient List<NodeInstance>[]

This is the aggregate data structure that is persisted to/from file

### NodeMap

    clusterNodes: Map: NodeId -> NodeInstance
    clusterNodes(): Iterable<NodeInstance>
    getOrCreate(NodeId): NodeInstance

  Maps a YARN NodeID record to a Slider `NodeInstance` structure

### NodeInstance

Every node in the cluster is modeled as an ragged array of `NodeEntry` instances, indexed
by role index -

    NodeEntry[roles]
    get(roleId): NodeEntry or null
    create(roleId): NodeEntry
    getNodeEntries(): NodeEntry[roles]
    getOrCreate(roleId): NodeEntry
    remove(roleId): NodeEntry

This could be implemented in a map or an indexed array; the array is more
efficient but it does mandate that the number of roles are bounded and fixed.

### NodeEntry

Records the details about all of a roles containers on a node. The
`active` field records the number of containers currently active.

    active: int
    requested: transient int
    releasing: transient int
    last_used: long

    NodeEntry.available(): boolean = active - releasing == 0 && requested == 0

The two fields `releasing` and `requested` are used to track the ongoing
state of YARN requests; they do not need to be persisted across freeze/thaw
cycles. They may be relevant across AM restart, but without other data
structures in the AM, not enough to track what the AM was up to before
it was restarted. The strategy will be to ignore unexpected allocation
responses (which may come from pre-restart) requests, while treating
unexpected container release responses as failures.

The `active` counter is only decremented after a container release response
has been received.

### RoleStatus

This is the existing `org.apache.hoya.yarn.appmaster.state.RoleStatus` class

### RoleList

A list mapping role to int enum is needed to index NodeEntry elements in
the NodeInstance arrays. Although such an enum is already implemented in the Slider
Providers, explicitly serializing and deserializing it would make
the persistent structure easier to parse in other tools, and resilient
to changes in the number or position of roles.

This list could also retain information about recently used/released nodes,
so that the selection of containers to request could shortcut a search


### ContainerPriority

The container priority field (a 32 bit integer) is used by Slider (0.5.x)
to index the specific role in a container so as to determine which role
has been offered in a container allocation message, and which role has
been released on a release event.

The Role History needs to track outstanding requests, so that
when an allocation comes in, it can be mapped back to the original request.
Simply looking up the nodes on the provided container and decrementing
its request counter is not going to work -the container may be allocated
on a different node from that requested.

**Proposal**: The priority field of a request is divided by Slider into 8 bits for
`roleID` and 24 bits for `requestID`. The request ID will be a simple
rolling integer -Slider will assume that after 2^24 requests per role, it can be rolled,
-though as we will be retaining a list of outstanding requests, a clash should not occur.
The main requirement  is: not have > 2^24 outstanding requests for instances of a specific role,
which places an upper bound on the size of a Slider cluster.

The splitting and merging will be implemented in a ContainerPriority class,
for uniform access.

### OutstandingRequest ###

Tracks an outstanding request. This is used to correlate an allocation response
(whose Container Priority file is used to locate this request), with the
node and role used in the request.

      roleId:  int
      requestID :  int
      node: string (may be null)
      requestedTime: long
      priority: int = requestID << 24 | roleId

The node identifier may be null -which indicates that a request was made without
a specific target node

### OutstandingRequestTracker ###

Contains a map from requestID to the specific `OutstandingRequest` made,
and generates the request ID

    nextRequestId: int
    requestMap(RequestID) -> OutstandingRequest

Operations

    addRequest(NodeInstance, RoleId): OutstandingRequest
        (and an updated request Map with a new entry)
    lookup(RequestID): OutstandingRequest
    remove(RequestID): OutstandingRequest
    listRequestsForNode(ClusterID): [OutstandingRequest]

The list operation can be implemented inefficiently unless it is found
to be important -if so a more complex structure will be needed.

### AvailableNodes

This is a field in `RoleHistory`

    availableNodes: List<NodeInstance>[]


For each role, lists nodes that are available for data-local allocation,
ordered by more recently released - To accelerate node selection

The performance benefit is most significant when requesting multiple nodes,
as the scan for M locations from N nodes is reduced from `M*N` comparisons
to 1 Sort + M list lookups.

Each list can be created off the Node Map by building for each role a sorted
list of all Nodes which are available for an instance of that role, 
using a comparator that places the most recently released node ahead of older
nodes.

This list is not persisted -when a Slider Cluster is frozen it is moot, and when
an AM is restarted this structure will be rebuilt.

1. When a node is needed for a new request, this list is consulted first.
1. After the request is issued it can be removed from the list
1. Whenever a container is released, if the node is now available for
requests for that node, should be added to to the front
of the list for that role.

If the list is empty during a container request operation, it means
that the Role History does not know of any nodes
in the cluster that have hosted instances of that role and which are not
in use. There are then two possible strategies to select a role

1. Ask for an instance anywhere in the cluster (policy in Slider 0.5)
1. Search the node map to identify other nodes which are (now) known about,
but which are not hosting instances of a specific role -this can be used
as the target for the next resource request.

Strategy #1 is simpler; Strategy #2 *may* decrease the affinity in the cluster,
as the AM will be explicitly requesting an instance on a node which it knows
is not running an instance of that role.


#### ISSUE What to do about failing nodes?

Should a node whose container just failed be placed at the
top of the stack, ready for the next request? 

If the container failed due to an unexpected crash in the application, asking
for that container back *is the absolute right strategy* -it will bring
back a new role instance on that machine. 

If the container failed because the node is now offline, the container request 
will not be satisfied by that node.

If there is a problem with the node, such that containers repeatedly fail on it,
then re-requesting containers on it will amplify the damage.

## Actions

### Bootstrap

1. Persistent Role History file not found; empty data structures created.

### Thaw

When thawing, the Role History should be loaded -if it is missing Slider
must revert to the bootstrap actions.

If found, the Role History will contain Slider's view of the Slider Cluster's
state at the time the history was saved, explicitly recording the last-used
time of all nodes no longer hosting a role's container. By noting which roles
were actually being served, it implicitly notes which nodes have a `last_used`
value greater than any of the `last_used` fields persisted in the file. That is:
all node entries listed as having active nodes at the time the history was
saved must have more recent data than those nodes listed as inactive.

When rebuilding the data structures, the fact that nodes were active at
save time must be converted into the data that indicates that the nodes
were at least in use *at the time the data was saved*. The state of the cluster
after the last save is unknown.

1: Role History loaded; Failure => Bootstrap.
2: Future: if role list enum != current enum, remapping could take place. Until then: fail.
3: Mark all nodes as active at save time to that of the

   //define a threshold
   threshold = rolehistory.saveTime - 7*24*60*60* 1000


    for (clusterId, clusternode) in rolehistory.clusterNodes().entries() :
      for (role, nodeEntry) in clusterNode.getNodeEntries():
        nodeEntry.requested = 0
        nodeEntry.releasing = 0
        if nodeEntry.active > 0 :
          nodeEntry.last_used = rolehistory.saveTime;
        nodeEntry.n.active = 0
        if nodeEntry.last_used < threshold :
          clusterNode.remove(role)
        else:
         availableNodes[role].add(clusterId)
       if clusterNode.getNodeEntries() isEmpty :
         rolehistory.clusterNodes.remove(clusterId)


    for availableNode in availableNodes:
      sort(availableNode,new last_used_comparator())

After this operation, the structures are purged with all out of date entries,
and the available node list contains a sorted list of the remainder.

### AM Restart


1: Create the initial data structures as the thaw operation
2: update the structure with the list of live nodes, removing those nodes
from the list of available nodes

    now = time()
    activeContainers = RM.getActiveContainers()

    for container in activeContainers:
       nodeId = container.nodeId
       clusterNode = roleHistory.nodemap.getOrCreate(nodeId)
       role = extractRoleId(container.getPriority)
       nodeEntry = clusterNode.getOrCreate(role)
       nodeEntry.active++
       nodeEntry.last_used = now
       availableNodes[role].remove(nodeId)

There's no need to resort the available node list -all that has happened
is that some entries have been removed


**Issue**: what if requests come in for a `(role, requestID)` for
the previous instance of the AM? Could we just always set the initial
requestId counter to a random number and hope the collision rate is very, very 
low (2^24 * #(outstanding_requests)). If YARN-1041 ensures that
a restarted AM does not receive outstanding requests, this issue goes away.


### Teardown

1. If dirty, save role history to its file.
1. Issue release requests
1. Maybe update data structures on responses, but do not mark Role History
as dirty or flush it to disk.

This strategy is designed to eliminate the expectation that there will ever
be a clean shutdown -and so that the startup-time code should expect
the Role History to have been written during shutdown. Instead the code
should assume that the history was saved to disk at some point during the life
of the Slider Cluster -ideally after the most recent change, and that the information
in it is only an approximate about what the previous state of the cluster was.

### Flex: Requesting a container in role `role`


    node = availableNodes[roleId].pop() 
    if node != null :
      node.nodeEntry[roleId].requested++;
    outstanding = outstandingRequestTracker.addRequest(node, roleId)
    request.node = node
    request.priority = outstanding.priority
      
    //update existing Slider role status
    roleStatus[roleId].incRequested();
      

There is a bias here towards previous nodes, even if the number of nodes
in the cluster has changed. This is why a node is picked where the number
of `active-releasing == 0 and requested == 0`, rather than where it is simply the lowest
value of `active + requested - releasing`: if there is no node in the nodemap that
is not running an instance of that role, it is left to the RM to decide where
the role instance should be instantiated.

This bias towards previously used nodes also means that (lax) requests
will be made of nodes that are currently unavailable either because they
are offline or simply overloaded with other work. In such circumstances,
the node will have an active count of zero -so the search will find these
nodes and request them -even though the requests cannot be satisfied.
As a result, the request will be downgraded to a rack-local or cluster-wide,
request -an acceptable degradation on a cluster where all the other entries
in the nodemap have instances of that specific node -but not when there are
empty nodes. 


#### Solutions

1. Add some randomness in the search of the datastructure, rather than simply
iterate through the values. This would prevent the same unsatisfiable
node from being requested first.

1. Keep track of requests, perhaps through a last-requested counter -and use
this in the selection process. This would radically complicate the selection
algorithm, and would not even distinguish "node recently released that was
also the last requested" from "node that has not recently satisfied requests
even though it was recently requested".
  
1. Keep track of requests that weren't satisfied, so identify a node that
isn't currently satisfying requests.


#### History Issues 

Without using that history, there is a risk that a very old assignment
is used in place of a recent one and the value of locality decreased.

But there are consequences:

**Performance**:

Using the history to pick a recent node may increase selection times on a
large cluster, as for every instance needed, a scan of all nodes in the
nodemap is required (unless there is some clever bulk assignment list being built
up), or a sorted version of the nodemap is maintained, with a node placed
at the front of this list whenever its is updated.

**Thaw-time problems**

There is also the risk that while thawing, the `rolehistory.saved`
flag may be updated while the cluster flex is in progress, so making the saved
nodes appear out of date. Perhaps the list of recently released nodes could
be rebuilt at thaw time.

The proposed `recentlyReleasedList` addresses this, though it creates
another data structure to maintain and rebuild at cluster thaw time
from the last-used fields in the node entries.

### AM Callback : onContainersAllocated 

    void onContainersAllocated(List<Container> allocatedContainers) 

This is the callback received when containers have been allocated.
Due to (apparently) race conditions, the AM may receive duplicate
container allocations -Slider already has to recognize this and 
currently simply discards any surplus.

If the AM tracks outstanding requests made for specific hosts, it
will need to correlate allocations with the original requests, so as to decrement
the node-specific request count. Decrementing the request count
on the allocated node will not work, as the allocation may not be
to the node originally requested.

    assignments = []
    operations =  []
    for container in allocatedContainers:
      cid = container.getId();
      roleId = container.priority & 0xff
      nodeId = container.nodeId
      outstanding = outstandingRequestTracker.remove(C.priority)
      roleStatus = lookupRoleStatus(container);
      roleStatus.decRequested();
      allocated = roleStatus.incActual();
      if outstanding == null || allocated > desired :
        operations.add(new ContainerReleaseOperation(cid))
        surplusNodes.add(cid);
        surplusContainers++
        roleStatus.decActual();
      else:
        assignments.add(new ContainerAssignment(container, role))
        node = nodemap.getOrCreate(nodeId)
        nodeentry = node.get(roleId)
        if nodeentry == null :
          nodeentry = new NodeEntry()
          node[roleId] = nodeentry
          nodeentry.active = 1
        else:
          if nodeentry.requested > 0 :
            nodeentry.requested--
          nodeentry.active++
        nodemap.dirty = true
    
        // work back from request ID to node where the 
        // request was outstanding
        requestID = outstanding != null? outstanding.nodeId : null
        if requestID != null:
          reqNode = nodeMap.get(requestID)
          reqNodeEntry = reqNode.get(roleId)
          reqNodeEntry.requested--
          if reqNodeEntry.available() :
            availableNodeList.insert(reqNodeEntry)


 
1. At end of this, there is a node in the nodemap, which has recorded that
there is now an active node entry for that role. The outstanding request has
been removed.

1. If a callback comes in for which there is no outstanding request, it is rejected
(logged, ignored, etc). This handles duplicate responses as well as any
other sync problem.

1. The node selected for the original request has its request for a role instance
decremented, so that it may be viewed as available again. The node is also
re-inserted into the AvailableNodes list -not at its head, but at its position
in the total ordering of the list.
 
### NMClientAsync Callback:  onContainerStarted()


    onContainerStarted(ContainerId containerId)
 
The AM uses this as a signal to remove the container from the list
of starting containers, moving it into the map of live nodes; the counters
in the associated `RoleInstance` are updated accordingly; the node entry
adjusted to indicate it has one more live node and one less starting node.

 
### NMClientAsync Callback:  onContainerStartFailed()


The AM uses this as a signal to remove the container from the list
of starting containers -the count of starting containers for the relevant
NodeEntry is decremented. If the node is now available for instances of this
container, it is returned to the queue of available nodes.


### Flex: Releasing a  role instance from the cluster

Simple strategy: find a node with at least one active container

    select a node N in nodemap where for NodeEntry[roleId]: active > releasing; 
    nodeentry = node.get(roleId)
    nodeentry.active--;

Advanced Strategy:

    Scan through the map looking for a node where active >1 && active > releasing.
    If none are found, fall back to the previous strategy

This is guaranteed to release a container on any node with >1 container in use,
if such a node exists. If not, the scan time has increased to #(nodes).

Once a node has been identified

1. a container on it is located (via the existing container map). This container
must: be of the target role, and not already be queued for release.
1. A release operation is queued trigger a request for the RM.
1. The (existing) `containersBeingReleased` Map has the container inserted into it

After the AM processes the request, it triggers a callback
 
### AM callback onContainersCompleted: 

    void onContainersCompleted(List<ContainerStatus> completedContainers)

This callback returns a list of containers that have completed.

These need to be split into successful completion of a release request
and containers which have failed. 

This is currently done by tracking which containers have been queued
for release, as well as which were rejected as surplus before even having
any role allocated onto them.

A container  is considered to  have failed if it  was an active  container which
has completed although it wasn't on the list of containers to release

    shouldReview = false
    for container in completedContainers:
      containerId = container.containerId
      nodeId = container.nodeId
      node = nodemap.get(nodeId)
      if node == null :
        // unknown node
        continue
      roleId = node.roleId
      nodeentry = node.get(roleId)
      nodeentry.active--
      nodemap.dirty = true
      if getContainersBeingReleased().containsKey(containerId) :
        // handle container completion
        nodeentry.releasing --
         
        // update existing Slider role status
        roleStatus[roleId].decReleasing();
        containersBeingReleased.remove(containerId)
      else: 
        //failure of a live node
        roleStatus[roleId].decActual();
        shouldReview = true
            
      if nodeentry.available():
        nodentry.last_used = now()
        availableNodes[roleId].insert(node)      
      //trigger a comparison of actual vs desired
    if shouldReview :
      reviewRequestAndReleaseNodes()

By calling `reviewRequestAndReleaseNodes()` the AM triggers
a re-evaluation of how many instances of each node a cluster has, and how many
it needs. If a container has failed and that freed up all role instances
on that node, it will have been inserted at the front of the `availableNodes` list.
As a result, it is highly likely that a new container will be requested on 
the same node. (The only way a node the list would be newer is 
be if other containers were completed in the same callback)



### Implementation Notes ###

Notes made while implementing the design.

`OutstandingRequestTracker` should also track requests made with
no target node; this makes seeing what is going on easier. `ARMClientImpl`
is doing something similar, on a priority-by-priority basis -if many
requests are made, each with their own priority, that base class's hash tables
may get overloaded. (it assumes a limited set of priorities)

Access to the role history datastructures was restricted to avoid
synchronization problems. Protected access is permitted so that a
test subclass can examine (and change?) the internals.

`NodeEntries need to add a launching value separate from active so that
when looking for nodes to release, no attempt is made to release
a node that has been allocated but is not yet live.

We can't reliably map from a request to a response. Does that matter?
If we issue a request for a host and it comes in on a different port, do we
care? Yes -but only because we are trying to track nodes which have requests
outstanding so as not to issue new ones. But if we just pop the entry
off the available list, that becomes moot.

Proposal: don't track the requesting numbers in the node entries, just
in the role status fields.

but: this means that we never re-insert nodes onto the available list if a
node on them was requested but not satisfied.

Other issues: should we place nodes on the available list as soon as all the entries
have been released?  I.e. Before YARN has replied

RoleStats were removed -left in app state. Although the rolestats would
belong here, leaving them where they were reduced the amount of change
in the `AppState` class, so risk of something breaking.

## MiniYARNCluster node IDs

Mini YARN cluster NodeIDs all share the same hostname , at least when running
against file://; so mini tests with >1 NM don't have a 1:1 mapping of
`NodeId:NodeInstance`. What will happen is that 
`NodeInstance getOrCreateNodeInstance(Container container) '
will always return the same (now shared) `NodeInstance`.

## Releasing Containers when shrinking a cluster

When identifying instances to release in a bulk downscale operation, the full
list of targets must be identified together. This is not just to eliminate
multiple scans of the data structures, but because the containers are not
released until the queued list of actions are executed -the nodes' release-in-progress
counters will not be incremented until after all the targets have been identified.

It also needs to handle the scenario where there are many role instances on a
single server -it should prioritize those. 


The NodeMap/NodeInstance/NodeEntry structure is adequate for identifying nodes,
at least provided there is a 1:1 mapping of hostname to NodeInstance. But it
is not enough to track containers in need of release: the AppState needs
to be able to work backwards from a NodeEntry to container(s) stored there.

The `AppState` class currently stores this data in a `ConcurrentMap<ContainerId, RoleInstance>`

To map from NodeEntry/NodeInstance to containers to delete, means that either
a new datastructure is created to identify containers in a role on a specific host
(e.g a list of ContainerIds under each NodeEntry), or we add an index reference
in a RoleInstance that identifies the node. We already effectively have that
in the container

### dropping any available nodes that are busy

When scanning the available list, any nodes that are no longer idle for that
role should be dropped from the list.

This can happen when an instance was allocated on a different node from
that requested.

### Finding a node when a role has instances in the cluster but nothing
known to be available

One condition found during testing is the following: 

1. A role has one or more instances running in the cluster
1. A role has no entries in its available list: there is no history of the 
role ever being on nodes other than which is currently in use.
1. A new instance is requested.

In this situation, the `findNodeForNewInstance` method returns null: there
is no recommended location for placement. However, this is untrue: all
nodes in the cluster `other` than those in use are the recommended nodes. 

It would be possible to build up a list of all known nodes in the cluster that
are not running this role and use that in the request, effectively telling the
AM to pick one of the idle nodes. By not doing so, we increase the probability
that another instance of the same role will be allocated on a node in use,
a probability which (were there capacity on these nodes and placement random), be
`1/(clustersize-roleinstances)`. The smaller the cluster and the bigger the
application, the higher the risk.

This could be revisited, if YARN does not support anti-affinity between new
requests at a given priority and existing ones: the solution would be to
issue a relaxed placement request listing all nodes that are in the NodeMap and
which are not running an instance of the specific role. [To be even more rigorous,
the request would have to omit those nodes for which an allocation has already been
made off the available list and yet for which no container has yet been
granted]. 


## Reworked Outstanding Request Tracker

The reworked request tracker behaves as follows

1. outstanding requests with specific placements are tracked by `(role, hostname)`
1. container assigments are attempted to be resolved against the same parameters.
1. If found: that request is considered satisfied *irrespective of whether or not
the request that satisfied the allocation was the one that requested that location.
1. When all instances of a specific role have been allocated, the hostnames of
all outstanding requests are returned to the available node list on the basis
that they have been satisifed elswhere in the YARN cluster. This list is
then sorted.

This strategy returns unused hosts to the list of possible hosts, while retaining
the ordering of that list in most-recent-first.

### Weaknesses

if one or more container requests cannot be satisifed, then all the hosts in
the set of outstanding requests will be retained, so all these hosts in the
will be considered unavailable for new location-specific requests.
This may imply that new requests that could be explicity placed will now only
be randomly placed -however, it is moot on the basis that if there are outstanding
container requests it means the RM cannot grant resources: new requests at the
same priority (i.e. same Slider Role ID) will not be granted either.

The only scenario where this would be different is if the resource requirements
of instances of the target role were decreated during a cluster flex such that
the placement could now be satisfied on the target host. This is not considered
a significant problem.

# Persistence

The initial implementation uses the JSON-formatted Avro format; while significantly
less efficient than a binary format, it is human-readable

Here are sequence of entries from a test run on a single node cluster; running 1 HBase Master
and two region servers.

Initial save; the instance of Role 1 (HBase master) is live, Role 2 (RS) is not.

    {"entry":{"org.apache.hoya.avro.RoleHistoryHeader":{"version":1,"saved":1384183475949,"savedx":"14247c3aeed","roles":3}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":1,"active":true,"last_used":0}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":2,"active":false,"last_used":0}}}
  
At least one RS is live: 
  
    {"entry":{"org.apache.hoya.avro.RoleHistoryFooter":{"count":2}}}{"entry":{"org.apache.hoya.avro.RoleHistoryHeader":{"version":1,"saved":1384183476010,"savedx":"14247c3af2a","roles":3}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":1,"active":true,"last_used":0}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":2,"active":true,"last_used":0}}}

Another entry is saved -presumably the second RS is now live, which triggered another write
  
    {"entry":{"org.apache.hoya.avro.RoleHistoryFooter":{"count":2}}}{"entry":{"org.apache.hoya.avro.RoleHistoryHeader":{"version":1,"saved":1384183476028,"savedx":"14247c3af3c","roles":3}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":1,"active":true,"last_used":0}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":2,"active":true,"last_used":0}}}

At this point the cluster was frozen and thawed. Slider does not save the cluster state
at freeze time, but does as it is rebuilt.

When the cluster is restarted, every node that was active for a role at the time the file was saved `1384183476028`
is given a last_used timestamp of that time. 

When the history is next saved, the master has come back onto the (single) node,
it is active while its `last_used` timestamp is the previous file's timestamp.
No region servers are yet live.

    {"entry":{"org.apache.hoya.avro.RoleHistoryFooter":{"count":2}}}{"entry":{"org.apache.hoya.avro.RoleHistoryHeader":{"version":1,"saved":1384183512173,"savedx":"14247c43c6d","roles":3}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":1,"active":true,"last_used":1384183476028}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":2,"active":false,"last_used":1384183476028}}}

Here a region server is live

    {"entry":{"org.apache.hoya.avro.RoleHistoryFooter":{"count":2}}}{"entry":{"org.apache.hoya.avro.RoleHistoryHeader":{"version":1,"saved":1384183512199,"savedx":"14247c43c87","roles":3}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":1,"active":true,"last_used":1384183476028}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":2,"active":true,"last_used":1384183476028}}}

And here, another region server has started. This does not actually change the contents of the file

    {"entry":{"org.apache.hoya.avro.RoleHistoryFooter":{"count":2}}}{"entry":{"org.apache.hoya.avro.RoleHistoryHeader":{"version":1,"saved":1384183512217,"savedx":"14247c43c99","roles":3}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":1,"active":true,"last_used":1384183476028}}}
    {"entry":{"org.apache.hoya.avro.NodeEntryRecord":{"host":"192.168.1.85","role":2,"active":true,"last_used":1384183476028}}}

The `last_used` timestamps will not be changed until the cluster is shrunk or thawed, as the `active` flag being set
implies that the server is running both roles at the save time of `1384183512217`.

## Resolved issues

> How best to distinguish at thaw time from nodes used just before thawing
from nodes used some period before? Should the RoleHistory simply forget
about nodes which are older than some threshold when reading in the history?

we just track last used times


> Is there a way to avoid tracking the outstanding requests?
 
No 
 
> What will the strategy of picking the most-recently-used node do if
that node creates the container and then fails to start it up. Do we need
to add blacklisting too? Or actually monitor the container start time, and
if a container hasn't been there for very long, don't pick it.

Startup failures drop the node from the ready-to-use list; the node is no longer
trusted. We don't blacklist it (yet)


> Should we prioritise a node that was used for a long session ahead of
a node that was used more recently for a shorter session? Maybe, but
it complicates selection as generating a strict order of nodes gets
significantly harder.

No: you need to start tracking aggregate execution time, for the last session.
In a stable state, all servers recorded in the history will have spread the
data amongst them, so its irrelevant.
