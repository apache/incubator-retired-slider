tuple=`slider status $1 | grep "info.am.web.url"`
FS=":"
url=`echo $tuple | awk '{split($0,array,": ")} END{print array[2]}'`
url="${url%,}"
url="${url%\"}"
url="${url#\"}"
url="${url}ws/v1/slider/publisher/slider/hbase-site.xml"
curl -k -o hbase-site.xml $url
