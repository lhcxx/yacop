package org.apache.hadoop.yarn.applications.yacop.utils;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.config.VolumeConfig;

public class TestUtils {

    public static YacopConfig mockYacopConfig(String name, String cmd, String image, double cpus, double mem, int instances, boolean localImage, List<VolumeConfig> volumeConfigList, String engineType) {
        YacopConfig yacopConfig = mock(YacopConfig.class, withSettings().serializable());

        when(yacopConfig.getName()).thenReturn(name);
        when(yacopConfig.getCmd()).thenReturn(cmd);
        when(yacopConfig.getEngineImage()).thenReturn(image);
        when(yacopConfig.getCpus()).thenReturn(cpus);
        when(yacopConfig.getMem()).thenReturn(mem);
        when(yacopConfig.getInstances()).thenReturn(instances);
        when(yacopConfig.isEngineLocalImage()).thenReturn(localImage);
        when(yacopConfig.getVolumeConfigs()).thenReturn(volumeConfigList);
        when(yacopConfig.getEngineType()).thenReturn(engineType);
        return yacopConfig;
    }

    public static VolumeConfig mockVolumeConfig(String containerPath, String hostPath, String mode) {
        VolumeConfig volumeConfig = mock(VolumeConfig.class,withSettings().serializable());
        when(volumeConfig.getContainerPath()).thenReturn(containerPath);
        when(volumeConfig.getHostPath()).thenReturn(hostPath);
        when(volumeConfig.getMode()).thenReturn(mode);
        return volumeConfig;
    }
}
