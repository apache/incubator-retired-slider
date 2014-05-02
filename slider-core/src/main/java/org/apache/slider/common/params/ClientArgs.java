/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.common.params;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;

import java.util.Collection;

/**
 * Slider Client CLI Args
 */

public class ClientArgs extends CommonArgs {

  /*
   
   All the arguments for specific actions
  
   */
  /**
   * This is not bonded to jcommander, it is set up
   * after the construction to point to the relevant
   * entry
   */
  private AbstractClusterBuildingActionArgs buildingActionArgs;
  private final ActionAMSuicideArgs actionAMSuicideArgs = new ActionAMSuicideArgs();
  private final ActionBuildArgs actionBuildArgs = new ActionBuildArgs();
  private final ActionCreateArgs actionCreateArgs = new ActionCreateArgs();
  private final ActionDestroyArgs actionDestroyArgs = new ActionDestroyArgs();
  private final ActionExistsArgs actionExistsArgs = new ActionExistsArgs();
  private final ActionFlexArgs actionFlexArgs = new ActionFlexArgs();
  private final ActionFreezeArgs actionFreezeArgs = new ActionFreezeArgs();
  private final ActionGetConfArgs actionGetConfArgs = new ActionGetConfArgs();
  private final ActionKillContainerArgs actionKillContainerArgs =
    new ActionKillContainerArgs();
  private final ActionListArgs actionListArgs = new ActionListArgs();
  private final ActionRegistryArgs actionRegistryArgs = new ActionRegistryArgs();
  private final ActionStatusArgs actionStatusArgs = new ActionStatusArgs();
  private final ActionThawArgs actionThawArgs = new ActionThawArgs();
  private final ActionVersionArgs actionVersionArgs = new ActionVersionArgs();
  private final ActionHelpArgs actionHelpArgs = new ActionHelpArgs();


  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {

    addActions(
      actionAMSuicideArgs,
      actionBuildArgs,
      actionCreateArgs,
      actionDestroyArgs,
      actionExistsArgs,
      actionFlexArgs,
      actionFreezeArgs,
      actionGetConfArgs,
      actionKillContainerArgs,
      actionListArgs,
      actionRegistryArgs,
      actionStatusArgs,
      actionThawArgs,
      actionHelpArgs,
      actionVersionArgs
              );
  }

  @Override
  public void applyDefinitions(Configuration conf) throws
                                                   BadCommandArgumentsException {
    super.applyDefinitions(conf);
    //RM
    if (getManager() != null) {
      log.debug("Setting RM to {}", getManager());
      conf.set(YarnConfiguration.RM_ADDRESS, getManager());
    }
    if ( getBasePath() != null ) {
      log.debug("Setting basePath to {}", getBasePath());
      conf.set(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH, getBasePath().toString());
    }
  }

  public AbstractClusterBuildingActionArgs getBuildingActionArgs() {
    return buildingActionArgs;
  }

  public ActionAMSuicideArgs getActionAMSuicideArgs() {
    return actionAMSuicideArgs;
  }

  public ActionBuildArgs getActionBuildArgs() {
    return actionBuildArgs;
  }

  public ActionCreateArgs getActionCreateArgs() {
    return actionCreateArgs;
  }

  public ActionDestroyArgs getActionDestroyArgs() {
    return actionDestroyArgs;
  }

  public ActionExistsArgs getActionExistsArgs() {
    return actionExistsArgs;
  }

  public ActionFlexArgs getActionFlexArgs() {
    return actionFlexArgs;
  }

  public ActionFreezeArgs getActionFreezeArgs() {
    return actionFreezeArgs;
  }

  public ActionGetConfArgs getActionGetConfArgs() {
    return actionGetConfArgs;
  }

  public ActionKillContainerArgs getActionKillContainerArgs() {
    return actionKillContainerArgs;
  }

  public ActionListArgs getActionListArgs() {
    return actionListArgs;
  }

  public ActionRegistryArgs getActionRegistryArgs() {
    return actionRegistryArgs;
  }

  public ActionStatusArgs getActionStatusArgs() {
    return actionStatusArgs;
  }

  public ActionThawArgs getActionThawArgs() {
    return actionThawArgs;
  }

  /**
   * Look at the chosen action and bind it as the core action for the operation.
   * In theory this could be done by introspecting on the list of actions and 
   * choosing it without the switch statement. In practise this switch, while
   * verbose, is easier to debug.
   * @throws SliderException bad argument or similar
   */
  @Override
  public void applyAction() throws SliderException {
    String action = getAction();
    if (SliderActions.ACTION_BUILD.equals(action)) {
      bindCoreAction(actionBuildArgs);
      //its a builder, so set those actions too
      buildingActionArgs = actionBuildArgs;
    } else if (SliderActions.ACTION_CREATE.equals(action)) {
      bindCoreAction(actionCreateArgs);
      //its a builder, so set those actions too
      buildingActionArgs = actionCreateArgs;

    } else if (SliderActions.ACTION_FREEZE.equals(action)) {
      bindCoreAction(actionFreezeArgs);

    } else if (SliderActions.ACTION_THAW.equals(action)) {
      bindCoreAction(actionThawArgs);

    } else if (SliderActions.ACTION_AM_SUICIDE.equals(action)) {
      bindCoreAction(actionAMSuicideArgs);

    } else if (SliderActions.ACTION_DESTROY.equals(action)) {
      bindCoreAction(actionDestroyArgs);

    } else if (SliderActions.ACTION_EXISTS.equals(action)) {
      bindCoreAction(actionExistsArgs);

    } else if (SliderActions.ACTION_FLEX.equals(action)) {
      bindCoreAction(actionFlexArgs);

    } else if (SliderActions.ACTION_GETCONF.equals(action)) {
      bindCoreAction(actionGetConfArgs);

    } else if (SliderActions.ACTION_HELP.equals(action) ||
               SliderActions.ACTION_USAGE.equals(action)) {
      bindCoreAction(actionHelpArgs);

    } else if (SliderActions.ACTION_KILL_CONTAINER.equals(action)) {
      bindCoreAction(actionKillContainerArgs);

    } else if (SliderActions.ACTION_LIST.equals(action)) {
      bindCoreAction(actionListArgs);

    } else if (SliderActions.ACTION_REGISTRY.equals(action)) {
      bindCoreAction(actionRegistryArgs);

    } else if (SliderActions.ACTION_STATUS.equals(action)) {
      bindCoreAction(actionStatusArgs);

    } else if (SliderActions.ACTION_VERSION.equals(action)) {
      bindCoreAction(actionVersionArgs);

    } else if (action == null || action.isEmpty()) {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_NO_ACTION);

    } else {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_UNKNOWN_ACTION
                                             + " " + action);
    }
  }
}
