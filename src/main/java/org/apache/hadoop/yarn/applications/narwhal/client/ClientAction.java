package org.apache.hadoop.yarn.applications.narwhal.client;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public interface ClientAction {

  public static String RUN = "run";
  public static String REGISTRY = "registry";
  public static String RESOLVE = "resolve";
  public static String PUBLISH = "publish";

  public boolean init(String[] args) throws ParseException;

  public boolean execute() throws YarnException, IOException, InterruptedException;

}
