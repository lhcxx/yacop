package org.apache.hadoop.yarn.applications.yacop.worker;


import org.apache.hadoop.yarn.applications.yacop.task.ExecutorID;
import org.apache.hadoop.yarn.applications.yacop.task.TaskId;

public class WorkerId extends ExecutorID {

  private TaskId taskId;

  public WorkerId(TaskId taskId, int id) {
    super(id);
    this.taskId = taskId;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public String toString() {
    return taskId.toString() + "_worker_" + id;
  }
}
