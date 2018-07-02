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
import org.agrona.concurrent.NoOpLock;

public class JobCreateService extends StubClusteredService {

  private AeronCluster clientCluster;

  long clusterSessionId;
  long correlationId;
  String msg;
  // simply echos cmd

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
      System.out.printf("\necho message %s", msg);
      while (session.offer(correlationId, buffer, offset, length) < 0) {
        Thread.yield();
      }

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
      final long msgCorrelationId = aeron.nextCorrelationId();
      final String newMessage = "WORLD_CREATED";
      msgBuffer.putInt(0, MessageIdentifier.JOB_CREATED.ordinal());
      msgBuffer.putStringWithoutLengthAscii(4, msg);

      // client sends message
      while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, msg.length() + 4) < 0)
      {
        Thread.yield();
      }

    }
    else if (messageIdentifier == MessageIdentifier.JOB_CREATED)
    {
      this.clusterSessionId = clusterSessionId;
      this.correlationId = correlationId;

      System.out.println("Add timer for job");
      if (!cluster.scheduleTimer(correlationId, timestampMs + 100)) {
        throw new IllegalStateException("unexpected back pressure");
      }
    }
    else
    {
      System.out.println("Ignore message " + msg);
    }
  }


  public void onTimerEvent(final long correlationId, final long timestampMs) {
    // timer has fired
    System.out.println("Timer fired!!!");
    // write JOB EXPIRED

//
//
//    final String responseMsg = msg + "-scheduled";
//    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
//    buffer.putStringWithoutLengthAscii(0, responseMsg);
//    final ClientSession clientSession = cluster.getClientSession(clusterSessionId);
//
//    while (clientSession.offer(correlationId, buffer, 0, responseMsg.length()) < 0) {
//      Thread.yield();
//    }
  }
}
