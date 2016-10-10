package org.apache.hadoop.yarn.applications.yacop.job;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.applications.yacop.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.yacop.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.yacop.event.JobEvent;
import org.apache.hadoop.yarn.applications.yacop.event.JobEventType;
import org.apache.hadoop.yarn.applications.yacop.event.TaskEvent;
import org.apache.hadoop.yarn.applications.yacop.event.TaskEventType;
import org.apache.hadoop.yarn.applications.yacop.job.JobId;
import org.apache.hadoop.yarn.applications.yacop.job.NJobImpl;
import org.apache.hadoop.yarn.applications.yacop.state.JobState;
import org.apache.hadoop.yarn.applications.yacop.state.TaskState;
import org.apache.hadoop.yarn.applications.yacop.task.NTaskImpl;
import org.apache.hadoop.yarn.applications.yacop.task.Task;
import org.apache.hadoop.yarn.applications.yacop.task.TaskId;
import org.apache.hadoop.yarn.applications.yacop.utils.TestUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;

import org.mockito.Matchers;

import static org.mockito.Mockito.withSettings;

public class TestNJobImpl {

  private static YarnConfiguration conf;
  private static TestingServer zkTestServer;
  private NJobImpl nJob;
  private YacopConfig yacopConfig;
  private AsyncDispatcher dispatcher;
  private ApplicationAttemptId applicationAttemptId;

  @BeforeClass
  public static void setupZk() throws Exception {
    conf = new YarnConfiguration();
    conf.set("hadoop.registry.zk.quorum", "localhost:12182");
    zkTestServer = new TestingServer(12182);
  }

  @Before
  public void setup() {
    applicationAttemptId = mock(ApplicationAttemptId.class);
    AppContext appContext = mock(AppContext.class);

    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    JobEventDispatcher jobEventDispatcher = new JobEventDispatcher(appContext);
    TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
    doNothing().when(taskEventDispatcher).handle(Matchers.any(TaskEvent.class));
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    when(applicationAttemptId.toString()).thenReturn("appattempt_1465186316357_0001_000001");
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.toString()).thenReturn("application_1465186316357_0001");
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    when(appContext.getEventHandler()).thenReturn(dispatcher.getEventHandler());
    nJob = new NJobImpl("Yacop job", applicationAttemptId, conf, appContext.getEventHandler());
    when(appContext.getJob()).thenReturn(nJob);
  }

  @Test
  public void testInitTransitionMultipleTasks() {
    double cpus = 2;
    double mem = 2048;
    int instances = 10;
    boolean localImage = false;
    createInitedJob(cpus, mem, instances, localImage);

    List<Task> tasks = nJob.getTasks();
    assertEquals(tasks.size(), instances);
    for (int i = 0; i < instances; i++) {
      Task task = tasks.get(i);
      NTaskImpl nTask = (NTaskImpl) task;
      assertNotNull(nTask.getYacopConfig().getName());
      assertNotNull(nTask.getYacopConfig().getCmd());
      assertNotNull(nTask.getYacopConfig().getEngineImage());
      assertEquals(nTask.getYacopConfig().getCpus(), cpus, 0.01);
      assertEquals(nTask.getYacopConfig().getMem(), mem, 0.01);
      assertEquals(nTask.getYacopConfig().isEngineLocalImage(), localImage);
    }
  }

  @Test
  public void testInitTransitionSingleTask() {
    double cpus = 2;
    double mem = 2048;
    int instances = 1;
    boolean localImage = false;
    createInitedJob(cpus, mem, instances, localImage);

    List<Task> tasks = nJob.getTasks();
    assertEquals(tasks.size(), instances);
    Task task = tasks.get(0);
    NTaskImpl nTask = (NTaskImpl) task;
    assertNotNull(nTask.getYacopConfig().getName());
    assertNotNull(nTask.getYacopConfig().getCmd());
    assertNotNull(nTask.getYacopConfig().getEngineImage());
    assertEquals(nTask.getYacopConfig().getCpus(), cpus, 0.01);
    assertEquals(nTask.getYacopConfig().getMem(), mem, 0.01);
    assertEquals(nTask.getYacopConfig().isEngineLocalImage(), localImage);
  }

  @Test
  public void testInitTransitionZeroTask() {
    double cpus = 2;
    double mem = 2048;
    int instances = 0;
    boolean localImage = false;
    createInitedJob(cpus, mem, instances, localImage);

    List<Task> tasks = nJob.getTasks();
    assertEquals(tasks.size(), instances);
  }

  @Test
  public void testStartTransition() {
    double cpus = 2;
    double mem = 2048;
    int instances = 10;
    boolean localImage = false;
    createStartedJob(cpus, mem, instances, localImage);
    assertEquals(nJob.getTasks().size(), instances);
  }

  @Test
  public void testCompleteTransitionSucess() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    double cpus = 2;
    double mem = 2048;
    int instances = 10;
    boolean localImage = false;
    createCompletedJob(cpus, mem, instances, localImage, true);
  }

  @Test
  public void testCompleteTransitionFailed() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    double cpus = 2;
    double mem = 2048;
    int instances = 10;
    boolean localImage = false;
    createCompletedJob(cpus, mem, instances, localImage, false);
  }

  private void createInitedJob(double cpus, double mem, int instances, boolean localImage) {
    YacopConfig yacopConfig = TestUtils.mockYacopConfig("simple-docker", "cat /proc/1/cgroup", "centos_yarn", cpus, mem, instances, localImage, null, "DOCKER");
    JobEvent jobEvent = new JobEvent(nJob.getID(), JobEventType.JOB_INIT);
    jobEvent.setYacopConfig(yacopConfig);
    nJob.handle(jobEvent);
    assertEquals(nJob.getStatus(), JobState.INITED);
  }

  private void createStartedJob(double cpus, double mem, int instances, boolean localImage) {
    createInitedJob(cpus, mem, instances, localImage);
    JobEvent jobEvent = new JobEvent(nJob.getID(), JobEventType.JOB_START);
    jobEvent.setYacopConfig(yacopConfig);
    nJob.handle(jobEvent);
    assertEquals(nJob.getStatus(), JobState.STARTED);
  }

  private void createCompletedJob(double cpus, double mem, int instances, boolean localImage, boolean allSuccess) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    createStartedJob(cpus, mem, instances, localImage);
    JobEvent jobEvent = new JobEvent(nJob.getID(), JobEventType.JOB_COMPLETED);
    jobEvent.setYacopConfig(yacopConfig);

    Field tasksField = nJob.getClass().getDeclaredField("tasks");
    tasksField.setAccessible(true);
    LinkedHashMap<TaskId, Task> tasks = new LinkedHashMap<>();
    for (int i = 0; i < instances; i++) {
      NTaskImpl task = mock(NTaskImpl.class);
      TaskId taskId = new TaskId(new JobId(applicationAttemptId), i + 1);
      when(task.getID()).thenReturn(taskId);
      if (allSuccess) {
        when(task.getStatus()).thenReturn(TaskState.SUCCEED);
      } else {
        when(task.getStatus()).thenReturn(TaskState.FAILED);
      }
      tasks.put(taskId, task);
      if (i != instances - 1) {
        nJob.handle(jobEvent);
        assertEquals(nJob.getStatus(), JobState.STARTED);
      } else {
        tasksField.set(nJob, tasks);
        nJob.handle(jobEvent);
        if (allSuccess) {
          assertEquals(nJob.getStatus(), JobState.SUCCEED);
        } else {
          assertEquals(nJob.getStatus(), JobState.FAILED);
        }
      }
    }
  }

  @After
  public void tearDown() throws IOException {

  }

  @AfterClass
  public static void tearDownZk() throws IOException {
//    zkTestServer.stop();
  }

}
