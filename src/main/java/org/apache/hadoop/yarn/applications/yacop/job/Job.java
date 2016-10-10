package org.apache.hadoop.yarn.applications.yacop.job;


import org.apache.hadoop.yarn.applications.yacop.task.Task;
import org.apache.hadoop.yarn.applications.yacop.task.TaskId;

public interface Job {
  String getName();
  JobId getID();
  Task getTask(TaskId taskId);
}
