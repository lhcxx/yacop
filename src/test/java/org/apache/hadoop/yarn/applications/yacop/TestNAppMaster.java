package org.apache.hadoop.yarn.applications.yacop;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.commons.cli.ParseException;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.yacop.event.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.applications.yacop.event.ContainerLauncherEventType;
import org.apache.hadoop.yarn.applications.yacop.event.JobEvent;
import org.apache.hadoop.yarn.applications.yacop.event.JobEventType;
import org.apache.hadoop.yarn.applications.yacop.job.NJobImpl;
import org.apache.hadoop.yarn.applications.yacop.service.ContainerAllocator;
import org.apache.hadoop.yarn.applications.yacop.service.ContainerLauncher;
import org.apache.hadoop.yarn.applications.yacop.state.JobState;
import org.apache.hadoop.yarn.applications.yacop.utils.TestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class TestNAppMaster {

  private NAppMaster nAppMaster;
  private JobEventDispatcher jobEventDispatcher;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private AsyncDispatcher dispatcher;
  private ApplicationAttemptId applicationAttemptId;
  private NJobImpl nJob;
  private static YarnConfiguration conf;
  private static TestingServer zkTestServer;
  private NAppMaster.AppContext context;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  
  @BeforeClass
  public static void setupZk() throws Exception {
    zkTestServer = new TestingServer(12181);
  }

  @Before
  public void setup() throws Exception {
    nAppMaster = new NAppMaster();

    context = mock(NAppMaster.AppContext.class);
    applicationAttemptId = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.toString()).thenReturn("appattempt_1465186316357_0001_000001");
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.toString()).thenReturn("application_1465186316357_0001");
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);

    dispatcher = new AsyncDispatcher();
    jobEventDispatcher = mock(JobEventDispatcher.class);
    containerAllocator = mock(ContainerAllocator.class);
    containerLauncher = mock(ContainerLauncher.class);
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    dispatcher.register(ContainerAllocatorEventType.class, containerAllocator);
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);

    Field applicationAttemptIdField = nAppMaster.getClass().getDeclaredField("applicationAttemptId");
    applicationAttemptIdField.setAccessible(true);
    applicationAttemptIdField.set(nAppMaster, applicationAttemptId);
    Field dispatcherField = nAppMaster.getClass().getDeclaredField("dispatcher");
    dispatcherField.setAccessible(true);
    dispatcherField.set(nAppMaster, dispatcher);
    Field jobEventDispatcherField = nAppMaster.getClass().getDeclaredField("jobEventDispatcher");
    jobEventDispatcherField.setAccessible(true);
    jobEventDispatcherField.set(nAppMaster, jobEventDispatcher);
    Field containerAllocatorField = nAppMaster.getClass().getDeclaredField("containerAllocator");
    containerAllocatorField.setAccessible(true);
    containerAllocatorField.set(nAppMaster, containerAllocator);
    Field containerLauncherField = nAppMaster.getClass().getDeclaredField("containerLauncher");
    containerLauncherField.setAccessible(true);
    containerLauncherField.set(nAppMaster, containerLauncher);

    doNothing().when(containerAllocator).start();
    doNothing().when(containerLauncher).start();
    doNothing().when(jobEventDispatcher).handle(Matchers.any(JobEvent.class));


    nJob = mock(NJobImpl.class);
    Field jobField = nAppMaster.getClass().getDeclaredField("job");
    jobField.setAccessible(true);
    jobField.set(nAppMaster, nJob);
    
    conf = new YarnConfiguration();
    conf.set("hadoop.registry.zk.quorum", "localhost:12181");
    
    Field confField = nAppMaster.getClass().getDeclaredField("conf");
    confField.setAccessible(true);
    confField.set(nAppMaster, conf);
  }

  @Test
  public void testInit() throws ParseException, IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    YacopConfig yacopConfig = TestUtils.mockYacopConfig("simple-docker", "cat /proc/1/cgroup", "centos_yarn", 1.0, 32, 2, false, null, "DOCKER");
    Field yacopConfigField = nAppMaster.getClass().getDeclaredField("yacopConfig");
    yacopConfigField.setAccessible(true);
    yacopConfigField.set(nAppMaster, yacopConfig);
    Field appContextField = nAppMaster.getClass().getDeclaredField("context");
    appContextField.setAccessible(true);
    appContextField.set(nAppMaster, context);
    when(context.getYacopConfig()).thenReturn(yacopConfig);
    nAppMaster.init(new String[]{});
    assertNotNull(dispatcher);
  }

  @Test
  public void testStart() throws Exception {
    nAppMaster.start();
    verify(containerAllocator, times(1)).start();
    verify(containerLauncher, times(1)).start();
    verify(jobEventDispatcher, times(1)).handle(Matchers.any(JobEvent.class));
  }

  @Test
  public void testFinishSucess() throws InterruptedException {
    when(nJob.getStatus()).thenReturn(JobState.SUCCEED);
    boolean finish = nAppMaster.finish();
    assertTrue(finish);
    verify(containerAllocator, times(1)).unregisterAM(Matchers.any(FinalApplicationStatus.class), Matchers.anyString(), Matchers.anyString());
    verify(containerAllocator, times(1)).stop();
    verify(containerLauncher, times(1)).stop();
  }

  @Test
  public void testFinishFail() throws InterruptedException {
    when(nJob.getStatus()).thenReturn(JobState.ERROR);
    boolean finish = nAppMaster.finish();
    assertFalse(finish);
    verify(containerAllocator, times(1)).unregisterAM(Matchers.any(FinalApplicationStatus.class), Matchers.anyString(), Matchers.anyString());
    verify(containerAllocator, times(1)).stop();
    verify(containerLauncher, times(1)).stop();
  }


  @After
  public void tearDown() {

  }
  
  @AfterClass
  public static void tearDownZk() throws IOException {
//    zkTestServer.stop();
  }

}