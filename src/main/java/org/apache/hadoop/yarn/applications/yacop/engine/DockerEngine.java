package org.apache.hadoop.yarn.applications.yacop.engine;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.applications.yacop.config.VolumeConfig;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Created by huichun.lu@intel.com on 16/8/28.
 */
public class DockerEngine implements YacopEngine {
  public ContainerLaunchContext buildContainerContext(Map<String, LocalResource> localResources, YacopConfig yacopConfig) {
    ContainerLaunchContext ctx = null;
    try {
      //env
      Map<String, String> env = new HashedMap();
      if (yacopConfig.getEngineType().equals("DOCKER")) {
        env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
        env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", yacopConfig.getEngineImage());
        if (yacopConfig.getVolumeConfigs() != null)
          env.put("YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS", getMountVolumePairList(yacopConfig));
        if (yacopConfig.getNetworkConfig() != null)
          env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK", yacopConfig.getNetworkConfig().getName());
      }
      List<String> commands = new ArrayList<>();
      //cmd
      Vector<CharSequence> vargs = new Vector<>(5);
      vargs.add("(" + yacopConfig.getCmd() + ")");
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }
      commands.add(command.toString());
      //tokens
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      //ctx
      ctx = ContainerLaunchContext.newInstance(
              localResources, env, commands, null, allTokens.duplicate(), null
      );
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ctx;
  }

  public String getMountVolumePairList(YacopConfig yacopConfig) {
    StringBuilder volumesList = new StringBuilder();
    String prefix = "";
    for (VolumeConfig vc : yacopConfig.getVolumeConfigs()) {
      volumesList.append(prefix);
      prefix = ",";
      volumesList.append(vc.getHostPath()).append(":").append(vc.getContainerPath());
    }
    return volumesList.toString();
  }
}