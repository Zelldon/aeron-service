package io.zeebe.aeron.service;

import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import io.zeebe.aeron.StubClusteredService;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

public class JobExpireService extends StubClusteredService {

  long clusterSessionId;
  long correlationId;
  String msg;

  public void onSessionMessage(
      final long clusterSessionId,
      final long correlationId,
      final long timestampMs,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final Header header) {

    this.msg = buffer.getStringWithoutLengthAscii(offset + 4, length - 4);
    final MessageIdentifier messageIdentifier = MessageIdentifier.values()[buffer.getInt(offset)];
    if (messageIdentifier == MessageIdentifier.JOB_CREATED)
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
      System.out.println("\nonExpireService Ignore message " + msg);
    }

  }

  public void onTimerEvent(final long correlationId, final long timestampMs) {
    final String responseMsg = msg + "-scheduled";
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    buffer.putStringWithoutLengthAscii(0, responseMsg);
    final ClientSession clientSession = cluster.getClientSession(clusterSessionId);

    while (clientSession.offer(correlationId, buffer, 0, responseMsg.length()) < 0) {
      Thread.yield();
    }
  }
}
