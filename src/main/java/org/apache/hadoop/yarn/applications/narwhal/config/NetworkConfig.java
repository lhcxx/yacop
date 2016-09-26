package org.apache.hadoop.yarn.applications.narwhal.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Arrays;

public class NetworkConfig implements Serializable {

  private final String mode;

  private final String name;

  private static String[] MODES = {"overlay"};

  private NetworkConfig(Builder builder) {
    this.mode = builder.mode;
    this.name = builder.name;
  }

  public String getMode() {
    return mode;
  }

  public String getName() {
    return name;
  }

  private static boolean isNetworkExist(String networkName) {
    String dockerCmd = "docker network ls";
    try {
      Process p = Runtime.getRuntime().exec(dockerCmd);
      p.waitFor();
      int exitCode = p.exitValue();
      StringBuilder networkInfo = new StringBuilder();
      if (exitCode == 0) {
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = null;
        while ((line = br.readLine()) != null) {
          networkInfo.append(line);
        }
      }
      String string = networkInfo.toString();
      boolean exists = string.contains(networkName);
      return exists;
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }

    return false;
  }

  static class Builder {

    private String mode;

    private String name;

    Builder mode(String mode) throws BuilderException {
      if (mode == null || mode.isEmpty())
        throw new BuilderException("network mode cannot be empty");
      if (!Arrays.asList(MODES).contains(mode))
        throw new BuilderException(mode + " does not supported");
      this.mode = mode;
      return this;
    }

    Builder name(String name) throws BuilderException {
      if (name == null || name.isEmpty())
        throw new BuilderException("network cannot be empty");
      if (!isNetworkExist(name))
        throw new BuilderException(name + " does not exist");
      this.name = name;
      return this;
    }

    synchronized NetworkConfig build() {
      return new NetworkConfig(this);
    }

  }

}
