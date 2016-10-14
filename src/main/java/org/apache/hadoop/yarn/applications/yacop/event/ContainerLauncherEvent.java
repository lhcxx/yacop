package org.apache.hadoop.yarn.applications.yacop.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.task.ExecutorID;
import org.apache.hadoop.yarn.event.AbstractEvent;


public class ContainerLauncherEvent extends AbstractEvent<ContainerLauncherEventType> {

  private ExecutorID id;

  private Container container;

  private String resourceFileName;

  private String resourceFilePath;

  private YacopConfig yacopConfig;

  public ContainerLauncherEvent(ExecutorID id, Container container,
                                ContainerLauncherEventType type) {
    super(type);
    this.id = id;
    this.container = container;
  }

  public ExecutorID getId() {
    return id;
  }

  public String toString() {
    return super.toString() + ", executorId: " + id;
  }

  public Container getContainer() {
    return container;
  }

  public String getResourceFileName() {
    return resourceFileName;
  }

  public void setResourceFileName(String resourceFileName) {
    this.resourceFileName = resourceFileName;
  }

  public String getResourceFilePath() {
    return resourceFilePath;
  }

  public void setResourceFilePath(String resourceFilePath) {
    this.resourceFilePath = resourceFilePath;
  }

  public YacopConfig getYacopConfig() {
    return yacopConfig;
  }

  public void setYacopConfig(YacopConfig yacopConfig) {
    this.yacopConfig = yacopConfig;
  }
}