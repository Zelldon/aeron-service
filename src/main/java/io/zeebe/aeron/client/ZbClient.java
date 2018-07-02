package io.zeebe.aeron.client;

import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.NoOpLock;

public class ZbClient {

    private AeronCluster aeronCluster;

    private final JobClient jobClient;

    private ZbClient()
    {
        aeronCluster = connectToCluster();

        jobClient = new JobClient(aeronCluster);
    }

    public static ZbClient newClient() {
        return new ZbClient();
    }

    public JobClient jobClient() {
        return jobClient;
    }

    private static AeronCluster connectToCluster()
    {
        return AeronCluster.connect(
                new AeronCluster.Context()
                        .ingressChannel("aeron:udp")
                        .clusterMemberEndpoints("localhost:9010", "localhost:9011", "localhost:9012")
                        .lock(new NoOpLock()));
    }

}
