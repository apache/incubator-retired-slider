How to create a Slider package?

Replace the placeholder tarball for Accumulo.
  cp ~/Downloads/accumulo-1.5.1-bin.tar.gz package/files/
  rm package/files/accumulo-1.5.1-bin.tar.gz.REPLACE

Create a zip package at the root of the package (<slider enlistment>/app-packages/accumulo-v1_5/) 
  zip -r accumulo_v151.zip .

Verify the content using  
  unzip -l "$@" accumulo_v151.zip

While appConfig.json and resources.json are not required for the package they work
well as the default configuration for Slider apps. So its advisable that when you
create an application package for Slider, include sample/default resources.json and
appConfig.json for a minimal Yarn cluster.
