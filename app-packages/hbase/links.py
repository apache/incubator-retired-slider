from __future__ import print_function
import logging
import json

file = open("links.json")
links = json.load(file)
file.close()
if links.has_key("entries"):
  entries = links["entries"]
  if entries.has_key("org.apache.slider.hbase.rest"):
    print("org.apache.slider.hbase.rest : %s" % entries["org.apache.slider.hbase.rest"])
  if entries.has_key("org.apache.slider.hbase.thrift"):
    print("org.apache.slider.hbase.thrift : %s" % entries["org.apache.slider.hbase.thrift"])
  if entries.has_key("org.apache.slider.hbase.thrift2"):
    print("org.apache.slider.hbase.thrift2 : %s" % entries["org.apache.slider.hbase.thrift2"])
