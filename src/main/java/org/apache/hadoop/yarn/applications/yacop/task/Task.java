package org.apache.hadoop.yarn.applications.yacop.task;

import org.apache.hadoop.yarn.applications.yacop.state.TaskState;
import org.apache.hadoop.yarn.applications.yacop.worker.Worker;
import org.apache.hadoop.yarn.applications.yacop.worker.WorkerId;

public interface Task {
  TaskId getID();
  TaskState getStatus();
  Worker getWorker(WorkerId workerId);
}
