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
import io.zeebe.aeron.service.JobCreateService;
import io.zeebe.aeron.service.JobExpireService;
import io.zeebe.aeron.service.MessageIdentifier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
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
      init();

      // test

      final Aeron aeron = aeronCluster.context().aeron();
      final SessionDecorator sessionDecorator = new SessionDecorator(aeronCluster.clusterSessionId());
      final Publication publication = aeronCluster.ingressPublication();

      final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer();
      final long msgCorrelationId = aeron.nextCorrelationId();
      final String msg = "Hello World!";
      msgBuffer.putInt(0, MessageIdentifier.JOB_CREATE.ordinal());
      msgBuffer.putStringWithoutLengthAscii(4, msg);

      // client sends message
      while (sessionDecorator.offer(publication, msgCorrelationId, msgBuffer, 0, msg.length() + 4) < 0)
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


              String message = buffer.getStringWithoutLengthAscii(offset + 4, length - 4);
              System.out.printf("\nClient on message %s", message);

              messageCount.value += 1;
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

      while (true)
      {
        if (adapter.poll() <= 0)
        {
          Thread.yield();
        }
      }

      // tear down
//      tearDown();
    }

  private static void init() {
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
//    container = launchEchoService();
//    ClusteredServiceContainer.launch(
//      new ClusteredServiceContainer.Context()
//        .clusteredService(new JobExpireService())
//        .errorHandler(Throwable::printStackTrace)
//        .deleteDirOnStart(true));

//
    ClusteredServiceContainer.launch(
      new ClusteredServiceContainer.Context()
        .clusteredService(new JobCreateService())
        .errorHandler(Throwable::printStackTrace)
        .deleteDirOnStart(true));

    aeronCluster = connectToCluster();
  }

  private static void tearDown() {
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
                } else {
                  System.out.printf("\nClient on message %s", message);
                }

                messageCount.value += 1;
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
    final ClusteredService echoService =
        new StubClusteredService() {
          public void onSessionMessage(
              final long clusterSessionId,
              final long correlationId,
              final long timestampMs,
              final DirectBuffer buffer,
              final int offset,
              final int length,
              final Header header) {
            final ClientSession session = cluster.getClientSession(clusterSessionId);

            final String msg = buffer.getStringWithoutLengthAscii(offset, length);
            System.out.printf("\necho message %s", msg);

            while (session.offer(correlationId, buffer, offset, length) < 0) {
              Thread.yield();
            }
          }
        };

    return ClusteredServiceContainer.launch(
      new ClusteredServiceContainer.Context()
        .clusteredService(echoService)
        .errorHandler(Throwable::printStackTrace)
        .deleteDirOnStart(true));
  }

  private static ClusteredServiceContainer launchTimedService()
  {
    final ClusteredService timedService = new StubClusteredService()
    {
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
        final Header header)
      {
        this.clusterSessionId = clusterSessionId;
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
        .errorHandler(Throwable::printStackTrace)
        .deleteDirOnStart(true));
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
