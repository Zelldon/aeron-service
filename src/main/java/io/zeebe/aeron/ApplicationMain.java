package io.zeebe.aeron;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressAdapter;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.client.SessionDecorator;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.NoOpLock;

public class ApplicationMain {

    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int FRAGMENT_LIMIT = 1;

    private static ClusteredMediaDriver clusteredMediaDriver;
    private static ClusteredServiceContainer container;
    private static AeronCluster aeronCluster;

    public static void main(String args[])
    {
      // init
      clusteredMediaDriver = ClusteredMediaDriver.launch(
        new MediaDriver.Context()
          .threadingMode(ThreadingMode.SHARED)
          .termBufferSparseFile(true)
          .errorHandler(Throwable::printStackTrace)
          .dirDeleteOnStart(true),
        new Archive.Context()
          .maxCatalogEntries(MAX_CATALOG_ENTRIES)
          .threadingMode(ArchiveThreadingMode.SHARED)
          .deleteArchiveOnStart(true),
        new ConsensusModule.Context()
          .errorHandler(Throwable::printStackTrace)
          .deleteDirOnStart(true));

      // start up
      container = launchEchoService();
      aeronCluster = connectToCluster();

      // test
      shouldEchoMessageViaService();

      // tear down
      CloseHelper.close(aeronCluster);
      CloseHelper.close(container);
      CloseHelper.close(clusteredMediaDriver);

      if (null != clusteredMediaDriver)
      {
        clusteredMediaDriver.consensusModule().context().deleteDirectory();
        clusteredMediaDriver.archive().context().deleteArchiveDirectory();
        clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();
      }
    }

  public static void shouldEchoMessageViaService()
  {
    final Aeron aeron = aeronCluster.context().aeron();
    final SessionDecorator sessionDecorator = new SessionDecorator(aeronCluster.clusterSessionId());
    final Publication publication = aeronCluster.ingressPublication();

    final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
    final long msgCorrelationId = aeron.nextCorrelationId();
    final String msg = "Hello World!";
    msgBuffer.putStringWithoutLengthAscii(0, msg);

    while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, msg.length()) < 0)
    {
      Thread.yield();
    }

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
                if (correlationId != msgCorrelationId) {
                  throw new IllegalStateException();
                }

                String message = buffer.getStringWithoutLengthAscii(offset, length);
                if (!message.equals(msg)) {
                  throw new IllegalStateException();
                }
                else
                {
                  System.out.printf("\nOn message %s", message);
                }

                messageCount.value += 1;
              }

              @Override
              public void sessionEvent(long l, long l1, EventCode eventCode, String s) {}

              @Override
              public void newLeader(long l, long l1, long l2, long l3, long l4, int i, String s) {}
            },
            aeronCluster.clusterSessionId(),
            aeronCluster.egressSubscription(),
            FRAGMENT_LIMIT);

    while (messageCount.get() == 0)
    {
      if (adapter.poll() <= 0)
      {
        Thread.yield();
      }
    }
  }

//  @Test(timeout = 10_000)
//  public void shouldScheduleEventInService()
//  {
//    container = launchTimedService();
//    aeronCluster = connectToCluster();
//
//    final Aeron aeron = aeronCluster.context().aeron();
//    final SessionDecorator sessionDecorator = new SessionDecorator(aeronCluster.clusterSessionId());
//    final Publication publication = aeronCluster.ingressPublication();
//
//    final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
//    final long msgCorrelationId = aeron.nextCorrelationId();
//    final String msg = "Hello World!";
//    msgBuffer.putStringWithoutLengthAscii(0, msg);
//
//    while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, msg.length()) < 0)
//    {
//      TestUtil.checkInterruptedStatus();
//      Thread.yield();
//    }
//
//    final MutableInteger messageCount = new MutableInteger();
//    final EgressAdapter adapter = new EgressAdapter(
//      new StubEgressListener()
//      {
//        public void onMessage(
//          final long correlationId,
//          final long clusterSessionId,
//          final long timestamp,
//          final DirectBuffer buffer,
//          final int offset,
//          final int length,
//          final Header header)
//        {
//          assertThat(correlationId, is(msgCorrelationId));
//          assertThat(buffer.getStringWithoutLengthAscii(offset, length), is(msg + "-scheduled"));
//
//          messageCount.value += 1;
//        }
//      },
//      aeronCluster.clusterSessionId(),
//      aeronCluster.egressSubscription(),
//      FRAGMENT_LIMIT);
//
//    while (messageCount.get() == 0)
//    {
//      if (adapter.poll() <= 0)
//      {
//        TestUtil.checkInterruptedStatus();
//        Thread.yield();
//      }
//    }
//  }

    private static ClusteredServiceContainer launchEchoService()
    {
      final ClusteredService echoService = new StubClusteredService()
      {
        public void onSessionMessage(
          final ClientSession session,
          final long correlationId,
          final long timestampMs,
          final DirectBuffer buffer,
          final int offset,
          final int length,
          final Header header)
        {
          while (session.offer(correlationId, buffer, offset, length) < 0)
          {
            Thread.yield();
          }
        }
      };

      return ClusteredServiceContainer.launch(
        new ClusteredServiceContainer.Context()
          .clusteredService(echoService)
          .errorHandler(Throwable::printStackTrace));
    }

    private static ClusteredServiceContainer launchTimedService()
    {
      final ClusteredService timedService = new StubClusteredService()
      {
        long clusterSessionId;
        long correlationId;
        String msg;

        public void onSessionMessage(
          final ClientSession session,
          final long correlationId,
          final long timestampMs,
          final DirectBuffer buffer,
          final int offset,
          final int length,
          final Header header)
        {
          this.clusterSessionId = session.id();
          this.correlationId = correlationId;
          this.msg = buffer.getStringWithoutLengthAscii(offset, length);

          if (!cluster.scheduleTimer(correlationId, timestampMs + 100))
          {
            throw new IllegalStateException("unexpected back pressure");
          }
        }

        public void onTimerEvent(final long correlationId, final long timestampMs)
        {
          final String responseMsg = msg + "-scheduled";
          final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
          buffer.putStringWithoutLengthAscii(0, responseMsg);
          final ClientSession clientSession = cluster.getClientSession(clusterSessionId);

          while (clientSession.offer(correlationId, buffer, 0, responseMsg.length()) < 0)
          {
            Thread.yield();
          }
        }
      };

      return ClusteredServiceContainer.launch(
        new ClusteredServiceContainer.Context()
          .clusteredService(timedService)
          .errorHandler(Throwable::printStackTrace));
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
