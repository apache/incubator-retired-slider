package org.apache.slider.providers.agent.application.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DockerContainerInputFile {
  protected static final Logger log = LoggerFactory
      .getLogger(DockerContainerInputFile.class);

  private String containerPath;
  private String fileLocalPath;

  public DockerContainerInputFile() {
  }

  public String getContainerMount() {
    return containerPath;
  }

  public void setContainerMount(String containerMount) {
    this.containerPath = containerMount;
  }

  public String getFileLocalPath() {
    return fileLocalPath;
  }

  public void setFileLocalPath(String fileLocalPath) {
    this.fileLocalPath = fileLocalPath;
  }

}