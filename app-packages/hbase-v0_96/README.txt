How to create a Slider package?

Replace the placeholder tarball for Accumulo.
  cp ~/Downloads/hbase-0.96.1-hadoop2-bin.tar.gz package/files/
  rm package/files/hbase-0.96.1-hadoop2-bin.tar.gz.REPLACE

Create a zip package at the root of the package (<slider enlistment>/app-packages/hbase-v0_96/) 
  zip -r hbase-v096.zip .

Verify the content using  
  unzip -l "$@" hbase-v096.zip

While appConfig.json and resources.json are not required for the package they work
well as the default configuration for Slider apps. So its advisable that when you
create an application package for Slider, include sample/default resources.json and
appConfig.json for a minimal Yarn cluster.
