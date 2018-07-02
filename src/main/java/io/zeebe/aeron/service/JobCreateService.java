package io.zeebe.aeron.service;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.SessionDecorator;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.Header;
import io.zeebe.aeron.StubClusteredService;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.NoOpLock;

import java.util.HashMap;

public class JobCreateService extends StubClusteredService {

  private final Long2ObjectHashMap currentJobs = new Long2ObjectHashMap<>();
  private AeronCluster clientCluster;

  @Override
  public void onSessionMessage(
      long clusterSessionId,
      long correlationId,
      long timestampMs,
      DirectBuffer buffer,
      int offset,
      int length,
      Header header) {

    final String msg = buffer.getStringWithoutLengthAscii(offset + 4, length - 4);

    final MessageIdentifier messageIdentifier = MessageIdentifier.values()[buffer.getInt(offset)];

    if (messageIdentifier == MessageIdentifier.JOB_CREATE)
    {
      // echo as ack
      final ClientSession session = cluster.getClientSession(clusterSessionId);
      System.out.printf("\nJob create %s", msg);
      while (session.offer(correlationId, buffer, offset, length) < 0) {
        Thread.yield();
      }

      // write job created
      final long newCorrelationId = cluster.aeron().nextCorrelationId();
      sendMessage(msg, newCorrelationId, MessageIdentifier.JOB_CREATED);
    }
    else if (messageIdentifier == MessageIdentifier.JOB_CREATED)
    {
      System.out.printf("\nJob created %s\n", msg);
      final long newCorrelationId = cluster.aeron().nextCorrelationId();

      // store job
      currentJobs.put(newCorrelationId, new Job());

      System.out.println("Schedule expiration timer for job");
      if (!cluster.scheduleTimer(newCorrelationId, timestampMs + 100)) {
        throw new IllegalStateException("unexpected back pressure");
      }
    }
    else if (messageIdentifier == MessageIdentifier.JOB_EXPIRED)
    {


    }
    else
    {
      System.out.println("Ignore message " + msg);
    }
  }

  public void onTimerEvent(final long correlationId, final long timestampMs) {
    // timer has fired
    System.out.println("Job expired!");
    Job expiredJob = (Job) currentJobs.remove(correlationId);

    System.out.println("Correlation id " + correlationId + " job started at " + expiredJob.getCreationTime());

    final long newCorrelationId = cluster.aeron().nextCorrelationId();
    sendMessage("Expired", newCorrelationId, MessageIdentifier.JOB_EXPIRED);
  }

  private void sendMessage(String msg, long correlationId, MessageIdentifier identifier) {
    if (clientCluster == null)
    {
      clientCluster = AeronCluster.connect(
        new AeronCluster.Context().aeron(cluster.aeron()));
    }

    // write job created
    final Aeron aeron = clientCluster.context().aeron();
    final SessionDecorator sessionDecorator = new SessionDecorator(clientCluster.clusterSessionId());
    final Publication publication = clientCluster.ingressPublication();

    final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    msgBuffer.putInt(0, identifier.ordinal());
    msgBuffer.putStringWithoutLengthAscii(4, msg);

    // client sends message
    while (sessionDecorator.offer(publication, correlationId, msgBuffer, 0, msg.length() + 4) < 0)
    {
      Thread.yield();
    }
  }



}
