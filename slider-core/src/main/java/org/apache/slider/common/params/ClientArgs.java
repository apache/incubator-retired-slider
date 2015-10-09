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
import org.apache.slider.common.tools.SliderUtils;
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
   * 
   * KEEP IN ALPHABETICAL ORDER
   */
  private AbstractClusterBuildingActionArgs buildingActionArgs;
  private final ActionAMSuicideArgs actionAMSuicideArgs = new ActionAMSuicideArgs();
  private final ActionBuildArgs actionBuildArgs = new ActionBuildArgs();
  private final ActionCreateArgs actionCreateArgs = new ActionCreateArgs();
  private final ActionDependencyArgs actionDependencyArgs = new ActionDependencyArgs();
  private final ActionDestroyArgs actionDestroyArgs = new ActionDestroyArgs();
  private final ActionDiagnosticArgs actionDiagnosticArgs = new ActionDiagnosticArgs();
  private final ActionExistsArgs actionExistsArgs = new ActionExistsArgs();
  private final ActionFlexArgs actionFlexArgs = new ActionFlexArgs();
  private final ActionFreezeArgs actionFreezeArgs = new ActionFreezeArgs();
  private final ActionHelpArgs actionHelpArgs = new ActionHelpArgs();
  private final ActionInstallPackageArgs actionInstallPackageArgs = new ActionInstallPackageArgs();
  private final ActionPackageArgs actionPackageArgs = new ActionPackageArgs();
  private final ActionClientArgs actionClientArgs = new ActionClientArgs();
  private final ActionInstallKeytabArgs actionInstallKeytabArgs = new ActionInstallKeytabArgs();
  private final ActionKeytabArgs actionKeytabArgs = new ActionKeytabArgs();
  private final ActionKillContainerArgs actionKillContainerArgs =
    new ActionKillContainerArgs();
  private final ActionListArgs actionListArgs = new ActionListArgs();
  private final ActionLookupArgs actionLookupArgs = new ActionLookupArgs();
  private final ActionRegistryArgs actionRegistryArgs = new ActionRegistryArgs();
  private final ActionResolveArgs actionResolveArgs = new ActionResolveArgs();
  private final ActionStatusArgs actionStatusArgs = new ActionStatusArgs();
  private final ActionThawArgs actionThawArgs = new ActionThawArgs();
  private final ActionUpdateArgs actionUpdateArgs = new ActionUpdateArgs();
  private final ActionVersionArgs actionVersionArgs = new ActionVersionArgs();
  private final ActionUpgradeArgs actionUpgradeArgs = new ActionUpgradeArgs();

  public ClientArgs(String[] args) {
    super(args);
  }

  public ClientArgs(Collection args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {

    addActions(
        actionPackageArgs,
        actionKeytabArgs,
        actionBuildArgs,
        actionDependencyArgs,
        actionCreateArgs,
        actionListArgs,
        actionStatusArgs,
        actionRegistryArgs,
        actionClientArgs,
        actionFlexArgs,
        actionDiagnosticArgs,
        actionFreezeArgs,
        actionThawArgs,
        actionUpdateArgs,
        actionUpgradeArgs,
        actionDestroyArgs,
        actionExistsArgs,
        actionLookupArgs,
        actionResolveArgs,
        actionKillContainerArgs,
        actionAMSuicideArgs,
        actionInstallPackageArgs,
        actionInstallKeytabArgs,
        actionVersionArgs,
        actionHelpArgs
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
    if (getBasePath() != null) {
      log.debug("Setting basePath to {}", getBasePath());
      conf.set(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH,
          getBasePath().toString());
    }
  }

  public ActionDiagnosticArgs getActionDiagnosticArgs() {
	  return actionDiagnosticArgs;
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

  public ActionInstallPackageArgs getActionInstallPackageArgs() { return actionInstallPackageArgs; }

  public ActionClientArgs getActionClientArgs() { return actionClientArgs; }

  public ActionPackageArgs getActionPackageArgs() { return actionPackageArgs; }

  public ActionInstallKeytabArgs getActionInstallKeytabArgs() { return actionInstallKeytabArgs; }

  public ActionKeytabArgs getActionKeytabArgs() { return actionKeytabArgs; }

  public ActionUpdateArgs getActionUpdateArgs() {
    return actionUpdateArgs;
  }

  public ActionUpgradeArgs getActionUpgradeArgs() {
    return actionUpgradeArgs;
  }

  public ActionCreateArgs getActionCreateArgs() {
    return actionCreateArgs;
  }

  public ActionDependencyArgs getActionDependencyArgs() {
    return actionDependencyArgs;
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

  public ActionKillContainerArgs getActionKillContainerArgs() {
    return actionKillContainerArgs;
  }

  public ActionListArgs getActionListArgs() {
    return actionListArgs;
  }

  public ActionLookupArgs getActionLookupArgs() {
    return actionLookupArgs;
  }

  public ActionRegistryArgs getActionRegistryArgs() {
    return actionRegistryArgs;
  }

  public ActionResolveArgs getActionResolveArgs() {
    return actionResolveArgs;
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
   * verbose, is easier to debug. And in JDK7, much simpler.
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

    } else if (SliderActions.ACTION_DEPENDENCY.equals(action)) {
      bindCoreAction(actionDependencyArgs);

    } else if (SliderActions.ACTION_DESTROY.equals(action)) {
      bindCoreAction(actionDestroyArgs);

    } else if (SliderActions.ACTION_DIAGNOSTICS.equals(action)) {
      bindCoreAction(actionDiagnosticArgs);

    } else if (SliderActions.ACTION_EXISTS.equals(action)) {
      bindCoreAction(actionExistsArgs);

    } else if (SliderActions.ACTION_FLEX.equals(action)) {
      bindCoreAction(actionFlexArgs);

    } else if (SliderActions.ACTION_HELP.equals(action)) {
      bindCoreAction(actionHelpArgs);

    } else if (SliderActions.ACTION_INSTALL_PACKAGE.equals(action)) {
      bindCoreAction(actionInstallPackageArgs);

    } else if (SliderActions.ACTION_KEYTAB.equals(action)) {
      bindCoreAction(actionKeytabArgs);

    } else if (SliderActions.ACTION_PACKAGE.equals(action)) {
      bindCoreAction(actionPackageArgs);

    } else if (SliderActions.ACTION_CLIENT.equals(action)) {
      bindCoreAction(actionClientArgs);

    } else if (SliderActions.ACTION_INSTALL_KEYTAB.equals(action)) {
      bindCoreAction(actionInstallKeytabArgs);

    } else if (SliderActions.ACTION_KILL_CONTAINER.equals(action)) {
      bindCoreAction(actionKillContainerArgs);

    } else if (SliderActions.ACTION_LIST.equals(action)) {
      bindCoreAction(actionListArgs);

    } else if (SliderActions.ACTION_LOOKUP.equals(action)) {
      bindCoreAction(actionLookupArgs);

    } else if (SliderActions.ACTION_REGISTRY.equals(action)) {
      bindCoreAction(actionRegistryArgs);

    } else if (SliderActions.ACTION_RESOLVE.equals(action)) {
      bindCoreAction(actionResolveArgs);

    } else if (SliderActions.ACTION_STATUS.equals(action)) {
      bindCoreAction(actionStatusArgs);

    } else if (SliderActions.ACTION_UPDATE.equals(action)) {
      bindCoreAction(actionUpdateArgs);

    } else if (SliderActions.ACTION_UPGRADE.equals(action)) {
      bindCoreAction(actionUpgradeArgs);

    } else if (SliderActions.ACTION_VERSION.equals(action)) {
      bindCoreAction(actionVersionArgs);

    } else if (SliderUtils.isUnset(action)) {
      bindCoreAction(actionHelpArgs);

    } else {
      throw new BadCommandArgumentsException(ErrorStrings.ERROR_UNKNOWN_ACTION
                                             + " " + action);
    }
  }
}
