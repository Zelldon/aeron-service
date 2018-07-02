package io.zeebe.aeron;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.concurrent.CountDownLatch;

public class EchoService implements ClusteredService {

  private static final int MESSAGE_COUNT = 10;

  private int messageCount;
  private final CountDownLatch latchOne;
  private final CountDownLatch latchTwo;
  Cluster cluster;

  EchoService(final CountDownLatch latchOne, final CountDownLatch latchTwo) {
    this.latchOne = latchOne;
    this.latchTwo = latchTwo;
  }

  int messageCount() {
    return messageCount;
  }

  public void onSessionMessage(
      final ClientSession session,
      final long correlationId,
      final long timestampMs,
      final DirectBuffer buffer,
      final int offset,
      final int length,
      final Header header) {
    while (session.offer(correlationId, buffer, offset, length) < 0) {
      Thread.yield();
//      cluster.idle();
    }

    ++messageCount;

    if (messageCount == MESSAGE_COUNT) {
      latchOne.countDown();
    }

    if (messageCount == (MESSAGE_COUNT * 2)) {
      latchTwo.countDown();
    }
  }

  public void onStart(final Cluster cluster) {
    this.cluster = cluster;
  }

  public void onSessionOpen(final ClientSession session, final long timestampMs) {}

  public void onSessionClose(
      final ClientSession session, final long timestampMs, final CloseReason closeReason) {}

  public void onTimerEvent(final long correlationId, final long timestampMs) {}

  public void onTakeSnapshot(final Publication snapshotPublication) {}

  public void onLoadSnapshot(final Image snapshotImage) {}

  public void onRoleChange(final Cluster.Role newRole) {}

  public void onSessionMessage(long l, long l1, long l2, DirectBuffer directBuffer, int i, int i1, Header header) {

  }

  public void onReplayBegin() {

  }

  public void onReplayEnd() {

  }

  public void onReady() {

  }
}
