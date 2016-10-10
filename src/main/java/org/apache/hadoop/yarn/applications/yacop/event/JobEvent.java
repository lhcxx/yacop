package org.apache.hadoop.yarn.applications.yacop.event;


import org.apache.hadoop.yarn.applications.yacop.config.YacopConfig;
import org.apache.hadoop.yarn.applications.yacop.job.JobId;
import org.apache.hadoop.yarn.event.AbstractEvent;


public class JobEvent extends AbstractEvent<JobEventType>{
  private JobId jobId;

  private YacopConfig yacopConfig;
  public JobEvent(JobId jobId, JobEventType type) {
    super(type);
    this.jobId = jobId;
  }
  public JobId getJobId() {
    return jobId;
  }

  public YacopConfig getYacopConfig() {
    return yacopConfig;
  }

  public void setYacopConfig(YacopConfig yacopConfig) {
    this.yacopConfig = yacopConfig;
  }
}
