package org.apache.hadoop.yarn.applications.yacop.engine;


import org.apache.hadoop.yarn.applications.yacop.config.VolumeConfig;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.utils.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by huichun.lu@intel.com on 16/8/29.
 */
public class TestDockerEngine {
  private DockerEngine dockerEngine;

  @Before
  public void setup() {
    dockerEngine = new DockerEngine();
  }

  @Test
  public void testGetMountVolumePairList() {
    List<VolumeConfig> volumeConfigList = new ArrayList<>();
    VolumeConfig test_volumeConfig_1 = TestUtils.mockVolumeConfig("/etc/a", "/var/data/a","RO");
    VolumeConfig test_volumeConfig_2 = TestUtils.mockVolumeConfig("/etc/b", "/var/data/b","RO");
    VolumeConfig test_volumeConfig_3 = TestUtils.mockVolumeConfig("/etc/c", "/var/data/c","RO");
    volumeConfigList.add(test_volumeConfig_1);
    volumeConfigList.add(test_volumeConfig_2);
    volumeConfigList.add(test_volumeConfig_3);
    YacopConfig yacopConfig = TestUtils.mockYacopConfig("simple-docker","cat /proc/1/cgroup","centos_yarn",1.0,32,2,false,volumeConfigList,"DOCKER");
    String expected = "/var/data/a:/etc/a,/var/data/b:/etc/b,/var/data/c:/etc/c";
    String actual = dockerEngine.getMountVolumePairList(yacopConfig);
    assertEquals(expected, actual);
  }
}
