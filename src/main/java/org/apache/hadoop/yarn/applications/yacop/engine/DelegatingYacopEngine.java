package org.apache.hadoop.yarn.applications.yacop.engine;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.event.ContainerLauncherEvent;
import org.apache.hadoop.yarn.applications.yacop.task.TaskId;
import org.apache.hadoop.yarn.applications.yacop.util.YacopException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by huichun.lu@intel.com on 16/9/21.
 */
public class DelegatingYacopEngine implements YacopEngine {
  private DockerEngine dockerEngine;
  private SimpleContainerEngine simpleContainerEngine;
  private YacopEngine yacopEngine;
  private Map<String, LocalResource> localResources;

  public DelegatingYacopEngine() {
    dockerEngine = new DockerEngine();
    simpleContainerEngine = new SimpleContainerEngine();
    yacopEngine = null;
    localResources = new HashMap<>();
  }

  public ContainerLaunchContext buildContainerContext(Map<String, LocalResource> localResources, YacopConfig yacopConfig) {
    try {
      return yacopEngine.buildContainerContext(localResources, yacopConfig);
    } catch (YacopException e) {
        e.printStackTrace();
      }
    return null;
  }

  public ContainerLaunchContext buildContainerContext(ContainerLauncherEvent event) {
    pickYacopEngine(event);
    return buildContainerContext(localResources, event.getYacopConfig());
  }

  public void pickYacopEngine(ContainerLauncherEvent event) {
    if (event.getYacopConfig().getEngineType().equals("SIMPLE")) {
      yacopEngine = simpleContainerEngine;
    } else if (event.getYacopConfig().getEngineType().equals("DOCKER")) {
        if (event.getId() instanceof TaskId) {
          yacopEngine = dockerEngine;
        } else {
          yacopEngine = simpleContainerEngine;
          setUpLocalResources(event);
        }
      }
  }

  private void setUpLocalResources(ContainerLauncherEvent event) {
    String resourceFileName = event.getResourceFileName();
    String resourcePath = event.getResourceFilePath();
    if (resourcePath != "") {
      FileSystem fs = null;
      try {
        fs = FileSystem.get(new YarnConfiguration());
        Path dst = new Path(fs.getHomeDirectory(), resourcePath);
        boolean exists = fs.exists(dst);
        if (exists) {
          FileStatus scFileStatus = fs.getFileStatus(dst);
          LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
            scFileStatus.getModificationTime());
          localResources.put(resourceFileName, scRsrc);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
