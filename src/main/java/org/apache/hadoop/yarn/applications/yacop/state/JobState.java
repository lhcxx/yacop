package org.apache.hadoop.yarn.applications.yacop.state;


public enum JobState {
  NEW,
  INITED,
  KILLED,
  STARTED,
  FAILED,
  SUCCEED,
  ERROR
}
