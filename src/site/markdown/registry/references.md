<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
  
# References

Service registration and discovery is a problem in distributed computing that has been explored for over thirty years, with 
[Birrell81]'s *Grapevine* system the first known implementation -though of 

# Papers

* **[Birrell81]** Birrell, A. et al, [*Grapevine: An exercise in distributed computing*](http://research.microsoft.com/apps/pubs/default.aspx?id=63661). Comm. ACM 25, 4 (Apr 1982), pp260-274. 
The first documented directory service; relied on service shutdown to resolve update operations.

* **[Das02]** [*SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol*](http://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf)
P2P gossip-style data sharing protocol with random liveness probes to address scalable liveness checking. Ceph uses similar liveness checking.

* **[Marti02]** Marti S. and Krishnam V., [*Carmen: A Dynamic Service Discovery Architecture*](http://www.hpl.hp.com/techreports/2002/HPL-2002-257), 

* **[Lampson86]** Lampson, B. [*Designing a Global Naming Service*](http://research.microsoft.com/en-us/um/people/blampson/36-GlobalNames/Acrobat.pdf). DEC. 
Distributed; includes an update protocol and the ability to add links to other parts of the tree. Also refers to [*Xerox Clearinghouse*](http://bitsavers.informatik.uni-stuttgart.de/pdf/xerox/parc/techReports/OPD-T8103_The_Clearinghouse.pdf), which apparently shipped.

* **[Mockapetris88]** Mockapetris, P. [*Development of the domain name system*](http://bnrg.eecs.berkeley.edu/~randy/Courses/CS268.F08/papers/31_dns.pdf). The history of DNS

* **[Schroeder84]** Schroeder, M.D. et al, [*Experience with Grapevine: The Growth of a Distributed System*](http://research.microsoft.com/apps/pubs/default.aspx?id=61509). Xerox.
Writeup of the experiences of using grapevine, with its eventual consistency and lack of idempotent message delivery called out -along with coverage of operations issues.

* **[van Renesse08]**  van Renesse, R. et al, [*Astrolabe: A Robust and Scalable Technology For Distributed System Monitoring, Management, and Data Mining*](http://www.cs.cornell.edu/home/rvr/papers/astrolabe.pdf). ACM Transactions on Computer Systems
Grandest P2P management framework to date; the work that earned Werner Vogel his CTO position at Amazon.
 
* **[van Steen86]** van Steen, M. et al, [*A Scalable Location Service for Distributed Objects*](http://www.cs.vu.nl/~ast/publications/asci-1996a.pdf). 
Vrije Universiteit, Amsterdam. Probably the first Object Request Broker



 