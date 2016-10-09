package org.apache.hadoop.yarn.applications.yacop.config;

import java.io.Serializable;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

public class EngineConfig implements Serializable {

    private final String type;

    private final String image;

    private final boolean localImage;

    private final NetworkConfig networkConfig;
    
    private final List<VolumeConfig> volumeConfigs ;

    private EngineConfig(Builder builder) {
        this.type = builder.type;
        this.image = builder.image;
        this.localImage = builder.localImage;
        this.networkConfig = builder.networkConfig;
        this.volumeConfigs = builder.volumeConfigs;
    }

    public String getType() {
        return type;
    }

    public String getImage() {
        return image;
    }

    boolean isLocalImage() {
        return localImage;
    }
    
    NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    public List<VolumeConfig> getVolumeConfigs() {
        return volumeConfigs;
    }

    static class Builder {

        private static final String ENGINE_DOCKER = "DOCKER";

        private static final String[] ENGINE_TYPES = {ENGINE_DOCKER};

        private String type;

        private String image;

        private boolean localImage = true;
        
        private NetworkConfig networkConfig;

        private List<VolumeConfig> volumeConfigs = new ArrayList<>();

        Builder type(String type) throws BuilderException {
            if (Arrays.asList(ENGINE_TYPES).contains(type))
                this.type = type;
            else
                throw new BuilderException("Invalid engine type");
            return this;
        }

        Builder image(String image) throws BuilderException {
            if (image.isEmpty())
                throw new BuilderException("Invalid image name");
            this.image = image;
            return this;
        }

        Builder localImage(boolean localImage) {
            this.localImage = localImage;
            return this;
        }

        Builder addVolume(VolumeConfig volumeConfig) {
            this.volumeConfigs.add(volumeConfig);
            return this;
        }
        
        Builder addNetwork(NetworkConfig networkConfig) {
            this.networkConfig = networkConfig;
            return this;
        }

        synchronized EngineConfig build() throws BuilderException {
            if (this.type.equals(ENGINE_DOCKER) && this.image == null)
                throw new BuilderException("missing image");
            return new EngineConfig(this);
        }
    }
}
