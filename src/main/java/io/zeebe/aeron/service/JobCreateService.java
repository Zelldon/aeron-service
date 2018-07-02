package io.zeebe.aeron.service;

import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import io.zeebe.aeron.StubClusteredService;
import org.agrona.DirectBuffer;

public class JobCreateService extends StubClusteredService {


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
    final ClientSession session = cluster.getClientSession(clusterSessionId);

    final String msg = buffer.getStringWithoutLengthAscii(offset + 4, length - 4);

    final MessageIdentifier messageIdentifier = MessageIdentifier.values()[buffer.getInt(offset)];
    if (messageIdentifier == MessageIdentifier.JOB_CREATE)
    {
      System.out.printf("\necho message %s", msg);
      while (session.offer(correlationId, buffer, offset, length) < 0) {
        Thread.yield();
      }
    }
    else
    {
      System.out.println("Ignore message " + msg);
    }
  }
}
