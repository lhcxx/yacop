package org.apache.hadoop.yarn.applications.yacop.worker;


import org.apache.hadoop.yarn.applications.yacop.state.WorkerState;

public interface Worker {
  WorkerId getID();
  WorkerState getStatus();
}
