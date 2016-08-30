package org.apache.hadoop.yarn.applications.yacop.engine;

import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.util.YacopException;

import java.util.Map;

/**
 * Created by huichun.lu@intel.com on 16/8/27.
 */
public interface YacopEngine {
  ContainerLaunchContext buildContainerContext(Map<String, LocalResource> localResources, YacopConfig yacopConfig)
    throws YacopException;
}
