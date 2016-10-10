package org.apache.hadoop.yarn.applications.yacop.task;


import org.apache.hadoop.yarn.applications.yacop.job.JobId;

public class TaskId extends ExecutorID {

  private JobId jobId;

  public JobId getJobId() {
    return jobId;
  }

  public TaskId(JobId jobId, int id) {
    super(id);
    this.jobId = jobId;
  }

  public String toString() {
    return this.jobId + "_task_" + this.id;
  }

}
