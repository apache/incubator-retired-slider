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

package org.apache.slider.server.appmaster;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A service for launching containers
 */
public class RoleLaunchService extends AbstractService {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleLaunchService.class);
  /**
   * How long to expect launcher threads to shut down on AM termination:
   * {@value}
   */
  public static final int LAUNCHER_THREAD_SHUTDOWN_TIME = 10000;
  /**
   * Map of launched threads.
   * These are retained so that at shutdown time the AM can signal
   * all threads to stop.
   *
   * However, we don't want to run out of memory even if many containers
   * get launched over time, so the AM tries to purge this
   * of the latest launched thread when the RoleLauncher signals
   * the AM that it has finished
   */
  private final Map<RoleLauncher, Thread> launchThreads =
    new HashMap<RoleLauncher, Thread>();

  /**
   * Callback to whatever has the task of actually running the container
   * start operation
   */
  private final ContainerStartOperation containerStarter;


  private final ProviderService provider;
  /**
   * Filesystem to use for the launch
   */
  private final SliderFileSystem fs;

  /**
   * Path in the launch filesystem that refers to a configuration directory
   * -the interpretation of it is left to the Provider
   */
  private final Path generatedConfDirPath;
  /**
   * Path in the launch filesystem that refers to a temp directory
   * which will be cleaned up at (some) time in the future
   */
  private final Path launcherTmpDirPath;

  /**
   * Thread group for the launchers; gives them all a useful name
   * in stack dumps
   */
  private final ThreadGroup launcherThreadGroup = new ThreadGroup("launcher");

  private Map<String, String> envVars;

  /**
   * Construct an instance of the launcher
   * @param startOperation the callback to start the opreation
   * @param provider the provider
   * @param fs filesystem
   * @param generatedConfDirPath path in the FS for the generated dir
   * @param envVars
   * @param launcherTmpDirPath
   */
  public RoleLaunchService(ContainerStartOperation startOperation,
                           ProviderService provider,
                           SliderFileSystem fs,
                           Path generatedConfDirPath,
                           Map<String, String> envVars, Path launcherTmpDirPath) {
    super("RoleLaunchService");
    containerStarter = startOperation;
    this.fs = fs;
    this.generatedConfDirPath = generatedConfDirPath;
    this.launcherTmpDirPath = launcherTmpDirPath;
    this.provider = provider;
    this.envVars = envVars;
  }

  @Override
  protected void serviceStop() throws Exception {
    joinAllLaunchedThreads();
    super.serviceStop();
  }

  /**
   * Start an asychronous launch operation
   * @param container container target
   * @param role role
   * @param clusterSpec cluster spec to use for template
   */
  public void launchRole(Container container,
                         RoleStatus role,
                         AggregateConf clusterSpec) {
    String roleName = role.getName();
    //emergency step: verify that this role is handled by the provider
    assert provider.isSupportedRole(roleName) : "unsupported role";
    RoleLaunchService.RoleLauncher launcher =
      new RoleLaunchService.RoleLauncher(container,
                                         role.getProviderRole(),
                                         clusterSpec,
                                         clusterSpec.getResourceOperations()
                                                    .getOrAddComponent(roleName),
                                         clusterSpec.getAppConfOperations()
                                                    .getOrAddComponent(roleName) );
    launchThread(launcher,
                 String.format("%s-%s", roleName,
                               container.getId().toString())
                );
  }


  public void launchThread(RoleLauncher launcher, String name) {
    Thread launchThread = new Thread(launcherThreadGroup,
                                     launcher,
                                     name);

    // launch and start the container on a separate thread to keep
    // the main thread unblocked
    // as all containers may not be allocated at one go.
    synchronized (launchThreads) {
      launchThreads.put(launcher, launchThread);
    }
    launchThread.start();
  }

  /**
   * Method called by a launcher thread when it has completed;
   * this removes the launcher of the map of active
   * launching threads.
   * @param launcher launcher that completed
   * @param ex any exception raised
   */
  public void launchedThreadCompleted(RoleLauncher launcher, Exception ex) {
    log.debug("Launched thread {} completed", launcher, ex);
    synchronized (launchThreads) {
      launchThreads.remove(launcher);
    }
  }

  /**
   Join all launched threads
   needed for when we time out
   and we need to release containers
   */
  private void joinAllLaunchedThreads() {


    //first: take a snapshot of the thread list
    List<Thread> liveThreads;
    synchronized (launchThreads) {
      liveThreads = new ArrayList<Thread>(launchThreads.values());
    }
    int size = liveThreads.size();
    if (size > 0) {
      log.info("Waiting for the completion of {} threads", size);
      for (Thread launchThread : liveThreads) {
        try {
          launchThread.join(LAUNCHER_THREAD_SHUTDOWN_TIME);
        } catch (InterruptedException e) {
          log.info("Exception thrown in thread join: " + e, e);
        }
      }
    }
  }


  /**
   * Thread that runs on the AM to launch a region server.
   */
  private class RoleLauncher implements Runnable {

    // Allocated container
    public final Container container;
    public  final String containerRole;
    private final MapOperations resourceComponent;
    private final MapOperations appComponent;
    private final AggregateConf instanceDefinition;
    public final ProviderRole role;

    public RoleLauncher(Container container,
                        ProviderRole role,
                        AggregateConf instanceDefinition,
                        MapOperations resourceComponent,
                        MapOperations appComponent) {
      assert container != null;
      assert role != null;
      assert resourceComponent != null;
      assert appComponent != null;
      this.container = container;
      this.containerRole = role.name;
      this.role = role;
      this.resourceComponent = resourceComponent;
      this.appComponent = appComponent;
      this.instanceDefinition = instanceDefinition;
    }

    @Override
    public String toString() {
      return "RoleLauncher{" +
             "container=" + container.getId() +
             ", containerRole='" + containerRole + '\'' +
             '}';
    }

    @Override
    public void run() {
      Exception ex = null;
      try {
        ContainerLauncher containerLauncher = new ContainerLauncher(getConfig(),
                                                                    fs,
                                                                    container);


        containerLauncher.setupUGI();
        containerLauncher.putEnv(envVars);

        log.debug("Launching container {} into role {}",
                  container.getId(),
                  containerRole);


        //now build up the configuration data
        Path containerTmpDirPath =
          new Path(launcherTmpDirPath, container.getId().toString());
        provider.buildContainerLaunchContext(containerLauncher,
            instanceDefinition,
            container,
            containerRole,
            fs,
            generatedConfDirPath,
            resourceComponent,
            appComponent,
            containerTmpDirPath
        );

        RoleInstance instance = new RoleInstance(container);
        String[] envDescription = containerLauncher.dumpEnvToString();

        String commandsAsString = containerLauncher.getCommandsAsString();
        log.info("Starting container with command: {}",
                 commandsAsString);

        instance.command = commandsAsString;
        instance.role = containerRole;
        instance.roleId = role.id;
        instance.environment = envDescription;
        containerStarter.startContainer(container,
                                        containerLauncher.completeContainerLaunch(),
                                        instance);
      } catch (Exception e) {
        log.error("Exception thrown while trying to start {}: {}",
            containerRole, e);
        ex = e;
      } finally {
        launchedThreadCompleted(this, ex);
      }
    }

  }
}
