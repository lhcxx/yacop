package org.apache.hadoop.yarn.applications.narwhal.client;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.applications.narwhal.common.NRegistryOperator;
import org.apache.hadoop.yarn.applications.narwhal.common.NarwhalConstant;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.*;

public class ActionDoPortMapping implements ClientAction {

  private static final Log LOG = LogFactory.getLog(ActionDoPortMapping.class);

  private Options opts;
  private String containerHost = "";
  private String containerId = "";
  private String containerPort = "";
  private String applicationId = "";
  private Configuration conf;
  private NRegistryOperator registryOperator;

  public ActionDoPortMapping() {
    conf = new YarnConfiguration();
    opts = new Options();
    opts.addOption("applicationId", true, "registry the service record");
    opts.addOption("containerHost", true, "specify the container host");
    opts.addOption("containerId", true, "specify the container id");
    opts.addOption("containerPort", true, "specify the container port");
  }

  @Override
  public boolean init(String[] args) throws ParseException {
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (!cliParser.hasOption("applicationId")) {
      throw new IllegalArgumentException("no application id specified");
    }
    applicationId = cliParser.getOptionValue("applicationId");

    if (!cliParser.hasOption("containerHost")) {
      throw new IllegalArgumentException("no container host specified");
    }
    containerHost = cliParser.getOptionValue("containerHost");

    if (!cliParser.hasOption("containerId")) {
      throw new IllegalArgumentException("no container id specified");
    }
    containerId = cliParser.getOptionValue("containerId");

    if (!cliParser.hasOption("containerPort")) {
      throw new IllegalArgumentException("no container port specified");
    }
    containerPort = cliParser.getOptionValue("containerPort");

    return true;
  }

  @Override
  public boolean execute() throws YarnException, IOException, InterruptedException {
    copyScriptToLocal();
    //TODO: check whether the containerId belong to applicationId
    String[] cmd = {"./publish.sh", containerHost, containerId, containerPort};
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(new File("/tmp"));
    Process p = pb.start();
    p.waitFor();
    int exitCode = p.exitValue();
    StringBuilder returnVal = new StringBuilder();
    if (exitCode == 0) {
      BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = null;
      while ((line = br.readLine()) != null) {
        returnVal.append(line);
      }
      String destPort = returnVal.toString();
      registryOperator = new NRegistryOperator(applicationId, conf);
      registryOperator.setContainerRecord(containerId, NarwhalConstant.PORT, destPort + "-->" + containerPort);
      registryOperator.updateContainer(containerId);
      return true;
    }
    LOG.info("Failed to do port mapping for container: " + containerId);
    return false;
  }

  private void copyScriptToLocal() throws IOException {
    File file = new File("/tmp/publish.sh");
    if (file.exists())
      return;
    InputStream inputStream = ActionDoPortMapping.class.getResourceAsStream("/script/publish.sh");
    OutputStream outputStream = new FileOutputStream("/tmp/publish.sh");
    IOUtils.copy(inputStream, outputStream);
    file.setExecutable(true);
    inputStream.close();
    outputStream.close();
  }

}
