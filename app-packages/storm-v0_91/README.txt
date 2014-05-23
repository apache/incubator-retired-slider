How to create a Slider package?

Replace the placeholder tarball for Storm.
  cp ~/Downloads/apache-storm-0.9.1.2.1.1.0-237.tar.gz package/files/
  rm package/files/apache-storm-0.9.1.2.1.1.0-237.tar.gz.REPLACE

Create a zip package at the root of the package (<slider enlistment>/app-packages/storm-v0_91/) 
  zip -r storm_v091.zip .

Verify the content using  
  unzip -l "$@" storm_v091.zip

While appConfig.json and resources.json are not required for the package they work
well as the default configuration for Slider apps. So its advisable that when you
create an application package for Slider, include sample/default resources.json and
appConfig.json for a minimal Yarn cluster.
