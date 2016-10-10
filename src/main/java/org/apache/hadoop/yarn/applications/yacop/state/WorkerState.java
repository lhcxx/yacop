package org.apache.hadoop.yarn.applications.yacop.state;


public enum WorkerState {
  NEW,
  SCHEDULED,
  RUNNING,
  SUCCEED,
  FAILED,
  KILLED,
  ERROR
}
