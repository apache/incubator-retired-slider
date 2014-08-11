DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#
tuple=`slider status $1 | grep "info.am.web.url"`
echo $tuple
FS=":"
url=`echo $tuple | awk '{split($0,array,": ")} END{print array[2]}'`
url="${url%,}"
url="${url%\"}"
url="${url#\"}"
siteurl="${url}ws/v1/slider/publisher/slider/hbase-site.xml"
curl -k -o hbase-site.dnld $siteurl
grep -v 'hbase.tmp.dir' hbase-site.dnld > hbase-site.xml

linksurl="${url}ws/v1/slider/publisher/slider/quicklinks"
curl -k -o links.json $linksurl
python $DIR/links.py
#| sed -e 's/\/\///g' | awk 'BEGIN { FS = ":" } ; { print $2 }'
