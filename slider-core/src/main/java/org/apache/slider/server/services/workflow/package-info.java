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

package org.apache.slider.server.services.workflow;

/**

 <h2>
 Introduction
 </h2>
 This package contains classes which can be aggregated to build up
 complex workflows of services: sequences of operations, callbacks
 and composite services with a shared lifespan.
 
 Core concepts:
 <ol>
 <li>
 Workflow service instances have a limited lifespan, and will self-terminate when
 they consider it time</li>
 <li>
 Workflow Services that have children implement the {@link org.apache.slider.server.services.workflow.ServiceParent}
 class, which provides (thread-safe) access to the children -allowing new children
 to be added, and existing children to be ennumerated
 </li>
 <li>
 Workflow Services are designed to be aggregated, to be composed to produce larger
 composite services which than perform ordered operations, notify other services
 when work has completed, and to propagate failure up the service hierarchy.
 </li>
 <li>
 Workflow Services may be subclassed to extend their behavior, or to use them
 in specific applications. Just as the standard {@link org.apache.hadoop.service.CompositeService}
 is often subclassed to aggregate child services, the {@link org.apache.slider.server.services.workflow.WorkflowCompositeService}
 can be used instead -adding the feature that failing services trigger automatic
 parent shutdown. If that is the desired operational mode of a class,
 swapping the composite service implementation may be sufficient to adopt it.
 </li>
 </ol>
 
 <h2>
 How do the workflow services differ from the standard <code>CompositeService</code>?
 </h2>
 
 The {@link org.apache.slider.server.services.workflow.WorkflowCompositeService}
 shares the same model of "child services, all inited and started together".
 Where it differs is that if any child service stops -either due to a failure
 or to an action which invokes that service's <code>stop()</code> method.
 
 In contrast, the original <code>CompositeService</code> class starts its children
 in its <code>start()</code> method, but does not listen or react to any
 child service halting. As a result, changes in child state are not detected
 or propagated.
 
 If a child service runs until completed -that is it will not be stopped until
 instructed to do so, and if it is only the parent service that attempts to
 stop the child, then this difference is unimportant. 
 
 However, if any service that depends upon all it child services running -
 and if those child services are written so as to stop when they fail, using
 the <code>WorkflowCompositeService</code> as a base class will enable the 
 parent service to be automatically notified of a child stopping.
 
 The {@link org.apache.slider.server.services.workflow.WorkflowSequenceService}
 resembles the composite service in API, but its workflow is different. It
 initializes and starts its children one-by-one, only starting the second after
 the first one succeeds, the third after the second, etc. If any service in
 the sequence fails, the parent <code>WorkflowSequenceService</code> stops, 
 reporting the same exception. 
 
 
 <h2>
 Other Workflow Services
 </h2>
 
 <ul>
 <li>{@link org.apache.slider.server.services.workflow.WorkflowEventNotifyingService }:
 Notifies callbacks when a workflow reaches a specific point (potentially after a delay).</li>
 <li>{@link org.apache.slider.server.services.workflow.ForkedProcessService}:
 Executes a process when started, and binds to the life of that process. When the
 process terminates, so does the service -and vice versa.</li>
 <li>{@link org.apache.slider.server.services.workflow.ClosingService}: Closes
 an instance of <code>Closeable</code> when the service is stopped. This
 is purely a housekeeping class.</></li>
 <li>{@link }: </li>
 </ul>

Lower-level classes 
 <ul>
 <li>{@link org.apache.slider.server.services.workflow.AbstractWorkflowExecutorService }:
 This is a base class for YARN services that use an {@link java.util.concurrent.ExecutorService}.
 for managing asynchronous operations: it stops the executor when the service is
 stopped.
 </li>
 <li>{@link org.apache.slider.server.services.workflow.ForkedProcessService}:
 Executes a process when started, and binds to the life of that process. When the
 process terminates, so does the service -and vice versa.</li>
 <li>{@link org.apache.slider.server.services.workflow.LongLivedProcess}:
 The inner class used to managed the forked process. When called directly it
 offers more features.</li>
 <li>{@link org.apache.slider.server.services.workflow.ClosingService}:
 A parameterized service to close the <code>Closeable</code> passed in -used for cleaning
 up references.</li>
 </ul>



 */
