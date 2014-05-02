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


# Release Process

Here is our release process.

### Before you begin

Check out the latest version of the develop branch,
run the tests. This should be done on a checked out
version of the code that is not the one you are developing on
(ideally, a clean VM), to ensure that you aren't releasing a slightly
modified version of your own, and that you haven't accidentally
included passwords or other test run details into the build resource
tree.

The `slider-funtest` functional test package is used to run functional
tests against a running Hadoop YARN cluster. It needs to be configured
according to the instructions in [testing](testing.html) to
create HBase and Accumulo clusters in the YARN cluster.

*Make sure that the functional tests are passing (and not being skipped) before
starting to make a release*



**Step #1:** Create a JIRA for the release, estimate 3h
(so you don't try to skip the tests)

    export SLIDER_RELEASE_JIRA=BUG-13927
    
**Step #2:** Check everything in. Git flow won't let you progress without this.

**Step #3:** Git flow: create a release branch

    export SLIDER_RELEASE=0.5.2
    
    git flow release start slider-$SLIDER_RELEASE

**Step #4:** in the new branch, increment those version numbers using (the maven
versions plugin)[http://mojo.codehaus.org/versions-maven-plugin/]

    mvn versions:set -DnewVersion=$SLIDER_RELEASE


**Step #5:** commit the changed POM files
  
    git add <changed files>
    git commit -m "$SLIDER_RELEASE_JIRA updating release POMs for $SLIDER_RELEASE"

  
**Step #6:** Do a final test run to make sure nothing is broken

In the `slider` directory, run:

    mvn clean install -DskipTests

Once everything is built- including .tar files, run the tests

    mvn test

This will run the functional tests as well as the `slider-core` tests.

It is wise to reset any VMs here, and on live clusters kill all running jobs.
This stops functional tests failing because the job doesn't get started before
the tests time out.

As the test run takes 30-60+ minutes, now is a good time to consider
finalizing the release notes.


**Step #7:** Build the release package

Run
    
    mvn clean site:site site:stage package -DskipTests



**Step #8:** validate the tar file

Look in `slider-assembly/target` to find the `.tar.gz` file, and the
expanded version of it. Inspect that expanded version to make sure that
everything looks good -and that the versions of all the dependent artifacts
look good too: there must be no `-SNAPSHOT` dependencies.


**Step #9:** Build the release notes

Create a a one-line plain text release note for commits and tags
And a multi-line markdown release note, which will be used for artifacts.


    Release against hadoop 2.4.0, HBase-0.98.1 and Accumulo 1.5.1 artifacts. 

The multi-line release notes should go into `slider/src/site/markdown/release_notes`.


These should be committed

    git add --all
    git commit -m "$SLIDER_RELEASE_JIRA updating release notes"

**Step #10:** End the git flow

Finish the git flow release, either in the SourceTree GUI or
the command line:

    
    git flow release finish slider-$SLIDER_RELEASE
    

On the command line you have to enter the one-line release description
prepared earlier.

You will now be back on the `develop` branch.

**Step #11:** update mvn versions

Switch back to `develop` and update its version number past
the release number


    export SLIDER_RELEASE=0.6.0-SNAPSHOT
    mvn versions:set -DnewVersion=$SLIDER_RELEASE
    git commit -a -m "$SLIDER_RELEASE_JIRA updating development POMs to $SLIDER_RELEASE"

**Step #12:** Push the release and develop branches to github 

    git push origin master develop 

(assuming that `origin` maps to `git@github.com:hortonworks/hoya.git`;
 you can check this with `git remote -v`


The `git-flow` program automatically pushes up the `release/slider-X.Y` branch,
before deleting it locally.

If you are planning on any release work of more than a single test run,
consider having your local release branch track the master.


**Step #13:** ### Release on github small artifacts

Browse to https://github.com/hortonworks/slider/releases/new

Create a new release on the site by following the instructions

Files under 5GB can be published directly. Otherwise, follow step 14

**Step #14:**  For releasing via an external CDN (e.g. Rackspace Cloud)

Using the web GUI for your particular distribution network, upload the
`.tar.gz` artifact

After doing this, edit the release notes on github to point to the
tar file's URL.

Example: 
    [Download slider-0.10.1-all.tar.gz](http://dffeaef8882d088c28ff-185c1feb8a981dddd593a05bb55b67aa.r18.cf1.rackcdn.com/slider-0.10.1-all.tar.gz)

**Step #15:** Announce the release 

**Step #16:** Finish the JIRA

Log the time, close the issue. This should normally be the end of a 
sprint -so wrap that up too.

**Step #17:** Get back to developing!

Check out the develop branch and purge all release artifacts

    git checkout develop
    git pull origin
    mvn clean
    
