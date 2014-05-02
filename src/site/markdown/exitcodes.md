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

# Client Exit Codes

Here are the exit codes returned 

Exit code values 1 and 2 are interpreted by YARN -in particular converting the
"1" value from an error into a successful shut down. Slider
converts the -1 error code from a forked process into `EXIT_MASTER_PROCESS_FAILED`;
no. 72.


    /**
     * 0: success
     */
    int EXIT_SUCCESS                    =  0;
    
    /**
     * -1: generic "false" response. The operation worked but
     * the result was not true
     */
    int EXIT_FALSE                      = -1;
    
    /**
     * Exit code when a client requested service termination:
     */
    int EXIT_CLIENT_INITIATED_SHUTDOWN  =  1;
    
    /**
     * Exit code when targets could not be launched:
     */
    int EXIT_TASK_LAUNCH_FAILURE        =  2;
    
    /**
     * Exit code when an exception was thrown from the service:
     */
    int EXIT_EXCEPTION_THROWN           = 32;
    
    /**
     * Exit code when a usage message was printed:
     */
    int EXIT_USAGE                      = 33;
    
    /**
     * Exit code when something happened but we can't be specific:
     */
    int EXIT_OTHER_FAILURE              = 34;
    
    /**
     * Exit code when a control-C, kill -3, signal was picked up:
     */
                                  
    int EXIT_INTERRUPTED                = 35;
    
    /**
     * Exit code when the command line doesn't parse:, or
     * when it is otherwise invalid.
     */
    int EXIT_COMMAND_ARGUMENT_ERROR     = 36;
    
    /**
     * Exit code when the configurations in valid/incomplete:
     */
    int EXIT_BAD_CONFIGURATION          = 37;
    
    /**
     * Exit code when the configurations in valid/incomplete:
     */
    int EXIT_CONNECTIVTY_PROBLEM        = 38;
    
    /**
     * internal error: {@value}
     */
    int EXIT_INTERNAL_ERROR = 64;
    
    /**
     * Unimplemented feature: {@value}
     */
    int EXIT_UNIMPLEMENTED =        65;
  
    /**
     * service entered the failed state: {@value}
     */
    int EXIT_YARN_SERVICE_FAILED =  66;
  
    /**
     * service was killed: {@value}
     */
    int EXIT_YARN_SERVICE_KILLED =  67;
  
    /**
     * timeout on monitoring client: {@value}
     */
    int EXIT_TIMED_OUT =            68;
  
    /**
     * service finished with an error: {@value}
     */
    int EXIT_YARN_SERVICE_FINISHED_WITH_ERROR = 69;
  
    /**
     * the application instance is unknown: {@value}
     */
    int EXIT_UNKNOWN_INSTANCE = 70;
  
    /**
     * the application instance is in the wrong state for that operation: {@value}
     */
    int EXIT_BAD_STATE =    71;
  
    /**
     * A spawned master process failed 
     */
    int EXIT_PROCESS_FAILED = 72;
  
    /**
     * The cluster failed -too many containers were
     * failing or some other threshold was reached
     */
    int EXIT_DEPLOYMENT_FAILED = 73;
  
    /**
     * The application is live -and the requested operation
     * does not work if the cluster is running
     */
    int EXIT_APPLICATION_IN_USE = 74;
  
    /**
     * There already is an application instance of that name
     * when an attempt is made to create a new instance
     */
    int EXIT_INSTANCE_EXISTS = 75;

## Other exit codes

YARN itself can fail containers, here are some of the causes we've seen


    143: Appears to be triggered by the container exceeding its cgroup memory
    limits
 
