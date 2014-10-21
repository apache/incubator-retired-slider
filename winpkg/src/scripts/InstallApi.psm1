### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### A set of basic PowerShell routines that can be used to install and
### manage Hadoop services on a single node. For use-case see install.ps1.
###

###
### Global variables
###
$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)

$FinalName = "slider-@slider.version@"

###############################################################################
###
### Installs Slider.
###
### Arguments:
###     component: Component to be installed, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     role: Space separated list of roles that should be installed.
###           (for example, "jobtracker historyserver" for mapreduce)
###
###############################################################################

function Install(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [String]
    [Parameter( Position=3, Mandatory=$false )]
    $roles
    )
{


    if ( $component -eq "slider" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"
        Write-Log "Checking the JAVA Installation."
        if( -not (Test-Path $ENV:JAVA_HOME\bin\java.exe))
        {
            Write-Log "JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist" "Failure"
            throw "Install: JAVA_HOME not set properly; $ENV:JAVA_HOME\bin\java.exe does not exist."
        }

        Write-Log "Checking the Hadoop Installation."
        if( -not (Test-Path $ENV:HADOOP_HOME\bin\winutils.exe))
        {

          Write-Log "HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist" "Failure"
          throw "Install: HADOOP_HOME not set properly; $ENV:HADOOP_HOME\bin\winutils.exe does not exist."
        }

	    ### $sliderInstallPath: the name of the folder containing the application, after unzipping
	    $sliderInstallPath = Join-Path $nodeInstallRoot $FinalName
		$sliderInstallToBin = Join-Path "$sliderInstallPath" "bin"

	    Write-Log "Installing Apache $FinalName to $sliderInstallPath"

        ### Create Node Install Root directory
        if( -not (Test-Path "$sliderInstallPath"))
        {
            Write-Log "Creating Node Install Root directory: `"$sliderInstallPath`""
            $cmd = "mkdir `"$sliderInstallPath`""
            Invoke-CmdChk $cmd
        }

		# Rename zip file and initialize parent directory of $sliderInstallPath
		Rename-Item "$HDP_RESOURCES_DIR\$FinalName-all.zip" "$HDP_RESOURCES_DIR\$FinalName.zip"
		$sliderIntallPathParent = (Get-Item $sliderInstallPath).parent.FullName

        ###
        ###  Unzip Slider distribution from compressed archive
        ###

        Write-Log "Extracting $FinalName.zip to $sliderIntallPathParent"
        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$HDP_RESOURCES_DIR\$FinalName.zip`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$sliderIntallPathParent`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$HDP_RESOURCES_DIR\$FinalName.zip")
            $destinationFolder = $shellApplication.NameSpace($sliderInstallPath)
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }

		###
        ### Set SLIDER_HOME environment variable
        ###
        Write-Log "Setting the SLIDER_HOME environment variable at machine scope to `"$sliderInstallPath`""
        [Environment]::SetEnvironmentVariable("SLIDER_HOME", $sliderInstallPath, [EnvironmentVariableTarget]::Machine)
        $ENV:SLIDER_HOME = "$sliderInstallPath"
        ###
        ### Installing Slider Hbase App package
        ###
        Write-Log "Installing Slider Hbase App package"
        $hbasePackage = Get-Item -Path "$HDP_RESOURCES_DIR\slider-hbase-app-win-package-*.zip"
        $hbasePackagePath = $hbasePackage.FullName
        $hbasePackageName = $hbasePackage.BaseName
        New-Item -ItemType Directory -Path "$sliderInstallPath\app-packages\$hbasePackageName" -Force -ErrorAction Stop |Out-Null
        Write-Log "Extracting $hbasePackagePath to $sliderIntallPath\app-packages\$hbasePackageName"
        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$hbasePackagePath`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$sliderIntallPath\app-packages\$hbasePackageName`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$hbasePackagePath")
            $destinationFolder = $shellApplication.NameSpace("$sliderIntallPath\app-packages\$hbasePackageName")
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }
        
        ###
        ### Installing Slider Storm App package
        ###
        Write-Log "Installing Slider Storm App package"
        $stormPackage = Get-Item -Path "$HDP_RESOURCES_DIR\slider-storm-app-win-package-*.zip"
        $stormPackagePath = $stormPackage.FullName
        $stormPackageName = $stormPackage.BaseName
        New-Item -ItemType Directory -Path "$sliderInstallPath\app-packages\$stormPackageName" -Force -ErrorAction Stop |Out-Null
        Write-Log "Extracting $stormPackagePath to $sliderIntallPath\app-packages\$stormPackageName"
        if ( Test-Path ENV:UNZIP_CMD )
        {
            ### Use external unzip command if given
            $unzipExpr = $ENV:UNZIP_CMD.Replace("@SRC", "`"$stormPackagePath`"")
            $unzipExpr = $unzipExpr.Replace("@DEST", "`"$sliderIntallPath\app-packages\$stormPackageName`"")
            ### We ignore the error code of the unzip command for now to be
            ### consistent with prior behavior.
            Invoke-Ps $unzipExpr
        }
        else
        {
            $shellApplication = new-object -com shell.application
            $zipPackage = $shellApplication.NameSpace("$stormPackagePath")
            $destinationFolder = $shellApplication.NameSpace("$sliderIntallPath\app-packages\$stormPackageName")
            $destinationFolder.CopyHere($zipPackage.Items(), 20)
        }
		
	    Write-Log "Finished installing Apache slider"
    }
    else
    {
        throw "Install: Unsupported component argument."
    }
}


###############################################################################
###
### Uninstalls Hadoop component.
###
### Arguments:
###     component: Component to be uninstalled, it can be "core, "hdfs" or "mapreduce"
###     nodeInstallRoot: Install folder (for example "C:\Hadoop")
###
###############################################################################

function Uninstall(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot
    )
{
    if ( $component -eq "slider" )
    {
        $HDP_INSTALL_PATH, $HDP_RESOURCES_DIR = Initialize-InstallationEnv $scriptDir "$FinalName.winpkg.log"

	    Write-Log "Uninstalling Apache slider $FinalName"
	    $sliderInstallPath = Join-Path $nodeInstallRoot $FinalName

        ### If Hadoop Core root does not exist exit early
        if ( -not (Test-Path $sliderInstallPath) )
        {
            return
        }

		
	    ###
	    ### Delete install dir
	    ###
	    $cmd = "rd /s /q `"$sliderInstallPath`""
	    Invoke-Cmd $cmd

        ### Removing SLIDER_HOME environment variable
        Write-Log "Removing the SLIDER_HOME environment variable"
        [Environment]::SetEnvironmentVariable( "SLIDER_HOME", $null, [EnvironmentVariableTarget]::Machine )

        Write-Log "Successfully uninstalled slider"

    }
    else
    {
        throw "Uninstall: Unsupported compoment argument."
    }
}

###############################################################################
###
### Start component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to start
###
###############################################################################
function StartService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Starting `"$component`" `"$roles`" services"

    if ( $component -eq "slider" )
    {
        Write-Log "StartService: slider services"
		CheckRole $roles @("slideragent")

        foreach ( $role in $roles -Split("\s+") )
        {
            Write-Log "Starting $role service"
            Start-Service $role
        }
    }
    else
    {
        throw "StartService: Unsupported component argument."
    }
}

###############################################################################
###
### Stop component services.
###
### Arguments:
###     component: Component name
###     roles: List of space separated service to stop
###
###############################################################################
function StopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $roles
    )
{
    Write-Log "Stopping `"$component`" `"$roles`" services"

    if ( $component -eq "slider" )
    {
        ### Verify that roles are in the supported set
        CheckRole $roles @("slideragent")
        foreach ( $role in $roles -Split("\s+") )
        {
            try
            {
                Write-Log "Stopping $role "
                if (Get-Service "$role" -ErrorAction SilentlyContinue)
                {
                    Write-Log "Service $role exists, stopping it"
                    Stop-Service $role
                }
                else
                {
                    Write-Log "Service $role does not exist, moving to next"
                }
            }
            catch [Exception]
            {
                Write-Host "Can't stop service $role"
            }

        }
    }
    else
    {
        throw "StartService: Unsupported compoment argument."
    }
}

###############################################################################
###
### Alters the configuration of the slider component.
###
### Arguments:
###     component: Component to be configured, it should be "slider"
###     nodeInstallRoot: Target install folder (for example "C:\Hadoop")
###     serviceCredential: Credential object used for service creation
###     configs:
###
###############################################################################
function Configure(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $component,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $nodeInstallRoot,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=2, Mandatory=$false )]
    $serviceCredential,
    [hashtable]
    [parameter( Position=3 )]
    $configs = @{},
    [bool]
    [parameter( Position=4 )]
    $aclAllFolders = $True
    )
{

    if ( $component -eq "slider" )
    {
        Write-Log "Configuring Slider"
		###
        ### Apply slider-client.xml configuration changes
        ###
        $sliderClientSiteXmlFile = Join-Path $ENV:SLIDER_HOME "conf\slider-client.xml"

        Write-Log "Updating slider config file $sliderClientSiteXmlFile"

        UpdateXmlConfig  $sliderClientSiteXmlFile @{
        "slider.zookeeper.quorum" = "$ENV:ZOOKEEPER_HOSTS";
        }
       
    }
    else
    {
        throw "Configure: Unsupported compoment argument."
    }
}


### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Gives full permissions on the folder to the given user
function GiveFullPermissions(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $folder,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $username,
    [bool]
    [Parameter( Position=2, Mandatory=$false )]
    $recursive = $false)
{
    Write-Log "Giving user/group `"$username`" full permissions to `"$folder`""
    $cmd = "icacls `"$folder`" /grant ${username}:(OI)(CI)F"
    if ($recursive) {
        $cmd += " /T"
    }
    Invoke-CmdChk $cmd
}

### Checks if the given space separated roles are in the given array of
### supported roles.
function CheckRole(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $roles,
    [array]
    [parameter( Position=1, Mandatory=$true )]
    $supportedRoles
    )
{
    foreach ( $role in $roles.Split(" ") )
    {
        if ( -not ( $supportedRoles -contains $role ) )
        {
            throw "CheckRole: Passed in role `"$role`" is outside of the supported set `"$supportedRoles`""
        }
    }
}

### Creates and configures the service.
function CreateAndConfigureHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service,
    [String]
    [Parameter( Position=1, Mandatory=$true )]
    $hdpResourcesDir,
    [String]
    [Parameter( Position=2, Mandatory=$true )]
    $serviceBinDir,
    [System.Management.Automation.PSCredential]
    [Parameter( Position=3, Mandatory=$true )]
    $serviceCredential
)
{
    if ( -not ( Get-Service "$service" -ErrorAction SilentlyContinue ) )
    {
		 Write-Log "Creating service `"$service`" as $serviceBinDir\$service.exe"
        $xcopyServiceHost_cmd = "copy /Y `"$HDP_RESOURCES_DIR\serviceHost.exe`" `"$serviceBinDir\$service.exe`""
        Invoke-CmdChk $xcopyServiceHost_cmd

        #Creating the event log needs to be done from an elevated process, so we do it here
        if( -not ([Diagnostics.EventLog]::SourceExists( "$service" )))
        {
            [Diagnostics.EventLog]::CreateEventSource( "$service", "" )
        }

        Write-Log "Adding service $service"
        if ($serviceCredential.Password.get_Length() -ne 0)
        {
            $s = New-Service -Name "$service" -BinaryPathName "$serviceBinDir\$service.exe" -Credential $serviceCredential -DisplayName "Apache Hadoop $service"
            if ( $s -eq $null )
            {
                throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
            }
        }
        else
        {
            # Separately handle case when password is not provided
            # this path is used for creating services that run under (AD) Managed Service Account
            # for them password is not provided and in that case service cannot be created using New-Service commandlet
            $serviceUserName = $serviceCredential.UserName
            $cred = $serviceCredential.UserName.Split("\")

            # Throw exception if domain is not specified
            if (($cred.Length -lt 2) -or ($cred[0] -eq "."))
            {
                throw "Environment is not AD or domain is not specified"
            }

            $cmd="$ENV:WINDIR\system32\sc.exe create `"$service`" binPath= `"$serviceBinDir\$service.exe`" obj= $serviceUserName DisplayName= `"Apache Hadoop $service`" "
            try
            {
                Invoke-CmdChk $cmd
            }
            catch
            {
                throw "CreateAndConfigureHadoopService: Service `"$service`" creation failed"
            }
        }

        $cmd="$ENV:WINDIR\system32\sc.exe failure $service reset= 30 actions= restart/5000"
        Invoke-CmdChk $cmd

        $cmd="$ENV:WINDIR\system32\sc.exe config $service start= disabled"
        Invoke-CmdChk $cmd

        Set-ServiceAcl $service
    }
    else
    {
        Write-Log "Service `"$service`" already exists, Removing `"$service`""
        StopAndDeleteHadoopService $service
        CreateAndConfigureHadoopService $service $hdpResourcesDir $serviceBinDir $serviceCredential
    }
}

### Stops and deletes the Hadoop service.
function StopAndDeleteHadoopService(
    [String]
    [Parameter( Position=0, Mandatory=$true )]
    $service
)
{
    Write-Log "Stopping $service"
    $s = Get-Service $service -ErrorAction SilentlyContinue

    if( $s -ne $null )
    {
        Stop-Service $service
        $cmd = "sc.exe delete $service"
        Invoke-Cmd $cmd
    }
}

### Helper routing that converts a $null object to nothing. Otherwise, iterating over
### a $null object with foreach results in a loop with one $null element.
function empty-null($obj)
{
   if ($obj -ne $null) { $obj }
}

### Helper routine that updates the given fileName XML file with the given
### key/value configuration values. The XML file is expected to be in the
### Hadoop format. For example:
### <configuration>
###   <property>
###     <name.../><value.../>
###   </property>
### </configuration>
function UpdateXmlConfig(
    [string]
    [parameter( Position=0, Mandatory=$true )]
    $fileName,
    [hashtable]
    [parameter( Position=1 )]
    $config = @{} )
{
    $xml = [xml] (Get-Content $fileName)

    foreach( $key in empty-null $config.Keys )
    {
        $value = $config[$key]
        $found = $False
        $xml.SelectNodes('/configuration/property') | ? { $_.name -eq $key } | % { $_.value = $value; $found = $True }
        if ( -not $found )
        {
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem = $xml.CreateElement("property")
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("name")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n    ")) | Out-Null
            $newItem.AppendChild($xml.CreateElement("value")) | Out-Null
            $newItem.AppendChild($xml.CreateWhitespace("`r`n  ")) | Out-Null
            $newItem.name = $key
            $newItem.value = $value
            $xml["configuration"].AppendChild($newItem) | Out-Null
            $xml["configuration"].AppendChild($xml.CreateWhitespace("`r`n")) | Out-Null
        }
    }
    $xml.Save($fileName)
    $xml.ReleasePath
}

###
### Public API
###
Export-ModuleMember -Function Install
Export-ModuleMember -Function Uninstall
Export-ModuleMember -Function Configure
Export-ModuleMember -Function StartService
Export-ModuleMember -Function StopService
