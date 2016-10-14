package org.apache.hadoop.yarn.applications.yacop.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.applications.yacop.NAppMaster;
import org.apache.hadoop.yarn.applications.yacop.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.config.VolumeConfig;
import org.apache.hadoop.yarn.applications.yacop.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.yacop.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.applications.yacop.event.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.applications.yacop.event.ContainerLauncherEvent;
import org.apache.hadoop.yarn.applications.yacop.event.ContainerLauncherEventType;
import org.apache.hadoop.yarn.applications.yacop.event.JobEventType;
import org.apache.hadoop.yarn.applications.yacop.job.NJobImpl;
import org.apache.hadoop.yarn.applications.yacop.service.ContainerLauncher.NMCallback;
import org.apache.hadoop.yarn.applications.yacop.task.ExecutorID;
import org.apache.hadoop.yarn.applications.yacop.task.TaskId;
import org.apache.hadoop.yarn.applications.yacop.utils.TestUtils;
import org.apache.hadoop.yarn.applications.yacop.worker.WorkerId;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

public class TestContainerLauncher {

  private ContainerLauncher containerLauncher;
  private ContainerAllocator containerAllocator;
  private AsyncDispatcher dispatcher;
  private NMClientAsync nmClientAsync;
  private NMCallback nmCallback;
  private NAppMaster nAppMaster;

  @Before
  public void setup() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    YarnConfiguration conf = new YarnConfiguration();

    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    JobEventDispatcher jobEventDispatcher = mock(JobEventDispatcher.class);
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    containerAllocator = mock(ContainerAllocator.class);
    dispatcher.register(ContainerAllocatorEventType.class, containerAllocator);
    doNothing().when(containerAllocator).handle(Matchers.any(ContainerAllocatorEvent.class));
    AppContext appContext = mock(AppContext.class);
    when(appContext.getEventHandler()).thenReturn(dispatcher.getEventHandler());

    NJobImpl nJob = mock(NJobImpl.class);
    when(appContext.getJob()).thenReturn(nJob);

    nAppMaster = new NAppMaster();
    YacopConfig yacopConfig = TestUtils.mockYacopConfig("simple-docker", "cat /proc/1/cgroup", "centos_yarn", 1.0, 32, 2, false, null, "DOCKER");
    Field yacopConfigField = nAppMaster.getClass().getDeclaredField("yacopConfig");
    yacopConfigField.setAccessible(true);
    yacopConfigField.set(nAppMaster, yacopConfig);
    Field appContextField = nAppMaster.getClass().getDeclaredField("context");
    appContextField.setAccessible(true);
    appContextField.set(nAppMaster, appContext);
    when(appContext.getYacopConfig()).thenReturn(yacopConfig);

    containerLauncher = new ContainerLauncher(appContext);
    nmCallback = containerLauncher.new NMCallback();

    mockNMClientAsync();
  }

  @Test
  public void testLaunchContainerByTask() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    TaskId taskId = mock(TaskId.class);
    ContainerId containerId = mock(ContainerId.class);
    when(taskId.getContainerId()).thenReturn(containerId);
    Container container = mock(Container.class);
    ContainerLauncherEvent containerLauncherEvent = new ContainerLauncherEvent(taskId, container, ContainerLauncherEventType.CONATAINERLAUNCHER_LAUNCH);
    String image = "centos_yarn";
    YacopConfig yacopConfig = TestUtils.mockYacopConfig("simple-docker","cat /proc/1/cgroup","centos_yarn",1.0,32,2,false,null,"DOCKER");
    containerLauncherEvent.setYacopConfig(yacopConfig);
    containerLauncher.processEvent(containerLauncherEvent);
    sleep(1000);
    Field scheduledContainersField = containerLauncher.getClass().getDeclaredField("scheduledContainers");
    scheduledContainersField.setAccessible(true);
    ConcurrentHashMap<ContainerId, ExecutorID> scheduledContainers = (ConcurrentHashMap<ContainerId, ExecutorID>) scheduledContainersField.get(containerLauncher);

    verify(nmClientAsync, times(1)).startContainerAsync(Matchers.any(Container.class), Matchers.any(ContainerLaunchContext.class));
    assertEquals(scheduledContainers.size(), 1);
    assertEquals(scheduledContainers.get(containerId), taskId);
  }

  @Test
  public void testLaunchContainerByWorker() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    WorkerId workerId = mock(WorkerId.class);
    ContainerId containerId = mock(ContainerId.class);
    when(workerId.getContainerId()).thenReturn(containerId);
    Container container = mock(Container.class);
    ContainerLauncherEvent containerLauncherEvent = new ContainerLauncherEvent(workerId, container, ContainerLauncherEventType.CONATAINERLAUNCHER_LAUNCH);
    String resourceFileName = "centos_yarn";
    String resourceFilePath = "centos_yarn_path";
    containerLauncherEvent.setResourceFileName(resourceFileName);
    containerLauncherEvent.setResourceFilePath(resourceFilePath);
    YacopConfig yacopConfig = TestUtils.mockYacopConfig("simple-docker","cat /proc/1/cgroup","centos_yarn",1.0,32,2,false,null,"DOCKER");
    containerLauncherEvent.setYacopConfig(yacopConfig);
    containerLauncher.processEvent(containerLauncherEvent);
    sleep(1000);
    Field scheduledContainersField = containerLauncher.getClass().getDeclaredField("scheduledContainers");
    scheduledContainersField.setAccessible(true);
    ConcurrentHashMap<ContainerId, ExecutorID> scheduledContainers = (ConcurrentHashMap<ContainerId, ExecutorID>) scheduledContainersField.get(containerLauncher);

    verify(nmClientAsync, times(1)).startContainerAsync(Matchers.any(Container.class), Matchers.any(ContainerLaunchContext.class));
    assertEquals(scheduledContainers.size(), 1);
    assertEquals(scheduledContainers.get(containerId), workerId);
  }

  @Test
  public void testOnContainerStartedInScheduled() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    ConcurrentHashMap<ContainerId, ExecutorID> scheduledContainers = new ConcurrentHashMap<>();
    int scheduledContainersSize = 3;
    ContainerId currentContainerId = null;
    for (int i = 0; i < scheduledContainersSize; i++) {
      TaskId taskId = mock(TaskId.class);
      currentContainerId = mock(ContainerId.class);
      when(taskId.getContainerId()).thenReturn(currentContainerId);
      scheduledContainers.put(currentContainerId, taskId);
    }
    Field scheduledContainersField = containerLauncher.getClass().getDeclaredField("scheduledContainers");
    scheduledContainersField.setAccessible(true);
    scheduledContainersField.set(containerLauncher, scheduledContainers);

    nmCallback.onContainerStarted(currentContainerId, null);
    sleep(1000);
    verify(containerAllocator, times(1)).handle(Matchers.any(ContainerAllocatorEvent.class));
  }

  @Test
  public void testOnContainerStartedWithoutScheduled() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    ConcurrentHashMap<ContainerId, ExecutorID> scheduledContainers = new ConcurrentHashMap<>();
    int scheduledContainersSize = 3;
    ContainerId currentContainerId = null;
    for (int i = 0; i < scheduledContainersSize; i++) {
      TaskId taskId = mock(TaskId.class);
      currentContainerId = mock(ContainerId.class);
      when(taskId.getContainerId()).thenReturn(currentContainerId);
      scheduledContainers.put(currentContainerId, taskId);
    }
    Field scheduledContainersField = containerLauncher.getClass().getDeclaredField("scheduledContainers");
    scheduledContainersField.setAccessible(true);
    scheduledContainersField.set(containerLauncher, scheduledContainers);

    nmCallback.onContainerStarted(mock(ContainerId.class), null);
    sleep(1000);
    verify(containerAllocator, times(0)).handle(Matchers.any(ContainerAllocatorEvent.class));
  }

  private void mockNMClientAsync() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    nmClientAsync = mock(NMClientAsync.class);
    Field nmClientAsyncField = containerLauncher.getClass().getDeclaredField("nmClientAsync");
    nmClientAsyncField.setAccessible(true);
    nmClientAsyncField.set(containerLauncher, nmClientAsync);
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() {

  }

}
