package org.apache.hadoop.yarn.applications.yacop.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.yacop.NAppMaster;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.config.VolumeConfig;
import org.apache.hadoop.yarn.applications.yacop.engine.DelegatingYacopEngine;
import org.apache.hadoop.yarn.applications.yacop.event.*;
import org.apache.hadoop.yarn.applications.yacop.task.ExecutorID;
import org.apache.hadoop.yarn.applications.yacop.task.TaskId;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class ContainerLauncher extends EventLoop implements EventHandler<ContainerLauncherEvent> {

  private static final Log LOG = LogFactory.getLog(ContainerLauncher.class);

  private NMClientAsync nmClientAsync;

  private NMClientAsync.CallbackHandler launchListener;

  private ConcurrentHashMap<ContainerId, ExecutorID> scheduledContainers =
      new ConcurrentHashMap<>();

  private DelegatingYacopEngine delegatingYacopEngine;

  public ContainerLauncher(NAppMaster.AppContext context) {
    super(context);
    delegatingYacopEngine = new DelegatingYacopEngine();
  }

  //TODO
  public void stopContainers() {

  }

  @Override
  public void stop() {
    stopContainers();
    nmClientAsync.stop();
  }

  @Override
  public void handle(ContainerLauncherEvent containerLauncherEvent) {
    try {
      eventQueue.put(containerLauncherEvent);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public void processEvent(AbstractEvent event) {
    ContainerLauncherEvent CLEvent = (ContainerLauncherEvent)event;
    LOG.info("Processing the event " + CLEvent);
    switch (CLEvent.getType()) {
      case CONATAINERLAUNCHER_LAUNCH:
        launchContainer(CLEvent);
        break;
      case CONTAINERLAUNCHER_COMPLETED:
        completeContainer();
        break;
      case CONTAINERLAUNCHER_CLEANUP:
        cleanupContainer();
        break;
    }
  }

  @Override
  public void startClientAsync() {
    createNMClientAsync().start();
  }

  protected NMClientAsync createNMClientAsync() {
    launchListener = new NMCallback();
    nmClientAsync = NMClientAsync.createNMClientAsync(launchListener);
    nmClientAsync.init(context.getConf());
    return nmClientAsync;
  }

  private void launchContainer(ContainerLauncherEvent event) {
    ContainerLaunchContext ctx = delegatingYacopEngine.buildContainerContext(event);
    if (ctx == null) {
      LOG.info("ContainerLaunchContext is null");
    } else {
      if (event.getContainer() == null) {
        LOG.info("Container is null:" + event.getId());
      }
      LOG.info(event.getId() + " used container " + event.getContainer().getId());
      nmClientAsync.startContainerAsync(event.getContainer(), ctx);
      scheduledContainers.put(event.getId().getContainerId(), event.getId());
    }
  }

  private void completeContainer() {
    LOG.info("complete container");
  }

  private void cleanupContainer() {
    LOG.info("stop container");
    //nmClientAysnc.stopContainerAsync()
  }

  class NMCallback implements NMClientAsync.CallbackHandler {

    private final Log LOG = LogFactory.getLog(NMCallback.class);

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
      LOG.info("NM - Container: " + containerId + " started");
      Iterator<ContainerId> it = scheduledContainers.keySet().iterator();
      while (it.hasNext()) {
        ContainerId scheduledContainerId = it.next();
        if (scheduledContainerId.equals(containerId)) {
          ExecutorID executorID = scheduledContainers.get(scheduledContainerId);
          //post event to ContainerAllocator to tell it one container has started
          ContainerAllocatorEvent event = new ContainerAllocatorEvent(executorID,
              ContainerAllocatorEventType.CONTAINERALLOCATOR_CONTAINER_STARTED);
          eventHandler.handle(event);
          //remove from schedulerContainer list
          it.remove();
        }
      }
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOG.info("NM - Container: " + containerId + "status received : " + containerStatus);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      LOG.info("NM - Container" + containerId + " stopped");
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable throwable) {
      LOG.info("NM - start container" + containerId + " encountered error");
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable throwable) {
      LOG.info("NM - stop container" + containerId + " encountered error");
    }
  }

}
