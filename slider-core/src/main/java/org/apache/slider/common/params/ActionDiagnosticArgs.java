package org.apache.slider.common.params;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(commandNames = {SliderActions.ACTION_DIAGNOSTIC},
commandDescription = SliderActions.DESCRIBE_ACTION_DIAGNOSTIC)
public class ActionDiagnosticArgs extends AbstractActionArgs
{
	  public static final String USAGE =
	      "Usage: " + SliderActions.ACTION_DIAGNOSTIC
	      + Arguments.ARG_CLIENT + "| "
	      + Arguments.ARG_SLIDER + " <appname> " + "| "
	      + Arguments.ARG_APPLICATION + " <appname> " + "| "
	      + Arguments.ARG_YARN + "| "
	      + Arguments.ARG_CREDENTIALS + "| "
	      + Arguments.ARG_ALL + " <appname> " + "| "
	      + Arguments.ARG_LEVEL + " <appname> "
	      + " [" + Arguments.ARG_VERBOSE + "] ";
	  
	@Override
	public String getActionName() {
		return SliderActions.ACTION_DIAGNOSTIC;
	}
	
	  @Parameter(names = {ARG_CLIENT}, 
	      description = "print configuration of the slider client")
	  public boolean client = false;
	
	  @Parameter(names = {ARG_SLIDER}, 
	      description = "print configuration of the running slider app master")
	  public String slider;
	
	  @Parameter(names = {ARG_APPLICATION}, 
	      description = "print configuration of the running application")
	  public String application;

	  @Parameter(names = {ARG_VERBOSE}, 
	      description = "print out information in details")
	  public boolean verbose = false;

	  @Parameter(names = {ARG_YARN}, 
	      description = "print configuration of the YARN cluster")
	  public boolean yarn = false;
	
	  @Parameter(names = {ARG_CREDENTIALS}, 
	      description = "print credentials of the current user")
	  public boolean credentials = false;
	
	  @Parameter(names = {ARG_ALL}, 
	      description = "print all of the information above")
	  public String all;
	
	  @Parameter(names = {ARG_LEVEL}, 
	      description = "diagnoze the application intelligently")
	  public String level;

	  /**
	   * Get the min #of params expected
	   * @return the min number of params in the {@link #parameters} field
	   */
	  @Override
	  public int getMinParams() {
	    return 0;
	  }
}
