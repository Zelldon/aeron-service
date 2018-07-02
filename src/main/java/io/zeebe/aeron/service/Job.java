package io.zeebe.aeron.service;

import java.util.Date;

public class Job {
  private final Date creationTime = new Date();

  public Date getCreationTime() {
    return creationTime;
  }
}
