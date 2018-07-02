package io.zeebe.aeron.client;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressAdapter;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.client.SessionDecorator;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import io.zeebe.aeron.service.Job;
import io.zeebe.aeron.service.MessageIdentifier;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;

import java.util.function.Consumer;

public class JobClient {

    private static final int FRAGMENT_LIMIT = 1;

    private final Aeron aeron;
    private final SessionDecorator sessionDecorator;
    private final Publication publication;
    private final AeronCluster aeronCluster;

    JobClient(AeronCluster aeronCluster)
    {
        this.aeronCluster = aeronCluster;
        aeron = this.aeronCluster.context().aeron();
        sessionDecorator = new SessionDecorator(aeronCluster.clusterSessionId());
        publication = aeronCluster.ingressPublication();
    }

    public void createJob()
    {
        final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
        final long msgCorrelationId = aeron.nextCorrelationId();
        msgBuffer.putInt(0, MessageIdentifier.JOB_CREATE.ordinal());

        // client sends message
        while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, 4) < 0)
        {
            Thread.yield();
        }
    }

    public void subscribe(Consumer<Job> handler)
    {
        final MutableInteger messageCount = new MutableInteger();
        final EgressAdapter adapter =
                new EgressAdapter(
                        new EgressListener() {
                            public void onMessage(
                                    final long correlationId,
                                    final long clusterSessionId,
                                    final long timestamp,
                                    final DirectBuffer buffer,
                                    final int offset,
                                    final int length,
                                    final Header header) {

                                // TODO serialize job from response
                                Job job = new Job();
                                handler.accept(job);
                            }

                            @Override
                            public void sessionEvent(long l, long l1, EventCode eventCode, String s) {
                                System.out.println(s);
                            }

                            @Override
                            public void newLeader(long l, long l1, long l2, long l3, long l4, int i, String s) {
                                System.out.println(s);
                            }
                        },
                        aeronCluster.clusterSessionId(),
                        aeronCluster.egressSubscription(),
                        FRAGMENT_LIMIT);

        new Thread(() -> {
            while (true)
            {
                if (adapter.poll() <= 0)
                {
                    Thread.yield();
                }
            }
        }).start();
    }


}
