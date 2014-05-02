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
  
# Specification of Slider behaviour

This is a a "more rigorous" definition of the behavior of Slider in terms
of its state and its command-line operations -by defining a 'formal' model
of HDFS, YARN and Slider's internal state, then describing the operations
that can take place in terms of their preconditions and postconditions.

This is to show what tests we can create to verify that an action
with a valid set of preconditions results in an outcome whose postconditions
can be verified. It also makes more apparent what conditions should be
expected to result in failures, as well as what the failure codes should be.

Specifying the behavior has also helped identify areas where there was ambiguity,
where clarification and more tests were needed.
 
The specification depends on ongoing work in [HADOOP-9361](https://issues.apache.org/jira/browse/HADOOP-9361): 
to define the Hadoop Filesytem APIs --This specification uses [the same notation](https://github.com/steveloughran/hadoop-trunk/blob/stevel/HADOOP-9361-filesystem-contract/hadoop-common-project/hadoop-common/src/site/markdown/filesystem/notation.md)

 
1. [Model: YARN And Slider](slider-model.html)
1. [CLI actions](cli-actions.html)

Exceptions and operations may specify exit codes -these are listed in
[Client Exit Codes](../exitcodes.html)
