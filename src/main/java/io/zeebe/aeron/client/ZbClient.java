package io.zeebe.aeron.client;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;

import org.agrona.CloseHelper;
import org.agrona.concurrent.NoOpLock;

public class ZbClient {

  private AeronCluster aeronCluster;
  private MediaDriver mediaDriver;

  private final JobClient jobClient;

  private ZbClient(String endpoints) {
    mediaDriver = launchMediaDriver();
    aeronCluster = connectToCluster(endpoints);

    jobClient = new JobClient(aeronCluster);
  }

  public static ZbClient newClient(String endPoints) {
    return new ZbClient(endPoints);
  }

  public JobClient jobClient() {
    return jobClient;
  }

  public void close() {
    CloseHelper.close(aeronCluster);
  }

  private static AeronCluster connectToCluster(String endpoints) {
    return AeronCluster.connect(
        new AeronCluster.Context()
            .ingressChannel("aeron:udp")
            .clusterMemberEndpoints(endpoints.split(","))
            .lock(new NoOpLock()));
  }

  private static MediaDriver launchMediaDriver() {
    return MediaDriver.launch(new MediaDriver.Context());
  }
}
