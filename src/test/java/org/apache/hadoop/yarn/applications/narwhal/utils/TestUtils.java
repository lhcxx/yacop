package org.apache.hadoop.yarn.applications.narwhal.utils;

import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.config.VolumeConfig;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.mock;

import java.util.List;

public class TestUtils {

    public static NarwhalConfig mockNarwhalConfig(String name, String cmd, String image, double cpus, double mem, int instances, boolean localImage, List<VolumeConfig> volumeConfigList, String engineType) {
        NarwhalConfig narwhalConfig = mock(NarwhalConfig.class, withSettings().serializable());

        when(narwhalConfig.getName()).thenReturn(name);
        when(narwhalConfig.getCmd()).thenReturn(cmd);
        when(narwhalConfig.getEngineImage()).thenReturn(image);
        when(narwhalConfig.getCpus()).thenReturn(cpus);
        when(narwhalConfig.getMem()).thenReturn(mem);
        when(narwhalConfig.getInstances()).thenReturn(instances);
        when(narwhalConfig.isEngineLocalImage()).thenReturn(localImage);
        when(narwhalConfig.getVolumeConfigs()).thenReturn(volumeConfigList);
        when(narwhalConfig.getEngineType()).thenReturn(engineType);
        return narwhalConfig;
    }

    public static VolumeConfig mockVolumeConfig(String containerPath, String hostPath, String mode) {
        VolumeConfig volumeConfig = mock(VolumeConfig.class,withSettings().serializable());
        when(volumeConfig.getContainerPath()).thenReturn(containerPath);
        when(volumeConfig.getHostPath()).thenReturn(hostPath);
        when(volumeConfig.getMode()).thenReturn(mode);
        return volumeConfig;
    }
}
