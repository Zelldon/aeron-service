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

public class JobService extends StubClusteredService {

  private final Long2ObjectHashMap currentJobs = new Long2ObjectHashMap<>();
  private AeronCluster clientCluster;
  private JobSubscriptionManager jobSubscriptionManager;

  @Override
  public void onStart(Cluster cluster) {
    super.onStart(cluster);
    jobSubscriptionManager = new JobSubscriptionManager(cluster);
  }

  @Override
  public void onSessionMessage(
      long clusterSessionId,
      long correlationId,
      long timestampMs,
      DirectBuffer buffer,
      int offset,
      int length,
      Header header) {

    final MessageIdentifier messageIdentifier = MessageIdentifier.values()[buffer.getInt(offset)];
    switch (messageIdentifier)
    {
      case JOB_CREATE:
        jobCreate(clusterSessionId, correlationId, buffer, offset, length);
        break;
      case JOB_CREATED:
        jobCreated(timestampMs);
        break;

      case JOB_COMPLETE:
        System.out.println("Got job complete from client!");
        // client completes job

        break;
      case JOB_EXPIRE:
        break;
      case JOB_EXPIRED:
        break;


      case JOB_ASSIGNED:
        final Job job = new Job();
        job.fromBuffer(buffer, offset, length);
        jobSubscriptionManager.assigned(job);

        break;
      case SUBSCRIBE:

        jobSubscriptionManager.subscribeClient(clusterSessionId);

        // write subscribed
        final long newCorrelationId = cluster.aeron().nextCorrelationId();
        sendMessage(newCorrelationId, MessageIdentifier.SUBSCRIBED);

        break;
      case SUBSCRIBED:

        for (Object value : currentJobs.values())
        {
          jobSubscriptionManager.assign((Job) value);
        }
        System.out.println("Subscribed successfully!");
        break;
    }
  }

  private void jobCreated(long timestampMs) {
    System.out.println("Job created.");
    final long newCorrelationId = cluster.aeron().nextCorrelationId();

    // store job
    final Job job = new Job();

    currentJobs.put(newCorrelationId, job);
    jobSubscriptionManager.assign(job);

    System.out.println("Schedule expiration timer for job");
    if (!cluster.scheduleTimer(newCorrelationId, timestampMs + 10_000)) {
      throw new IllegalStateException("unexpected back pressure");
    }
  }

  private void jobCreate(long clusterSessionId, long correlationId, DirectBuffer buffer, int offset, int length) {
    // echo as ack
    final ClientSession session = cluster.getClientSession(clusterSessionId);
    System.out.printf("\nJob create.");
    while (session.offer(correlationId, buffer, offset, length) < 0) {
      Thread.yield();
    }

    // write job created
    final long newCorrelationId = cluster.aeron().nextCorrelationId();
    final Job job = new Job();
    sendMessage(job, newCorrelationId);
  }

  @Override
  public void onTimerEvent(final long correlationId, final long timestampMs) {
    // timer has fired
//    System.out.println("Job expired!");
    Job expiredJob = (Job) currentJobs.remove(correlationId);

//    System.out.println("Correlation id " + correlationId + " job started at " + expiredJob.getCreationTime());

    final long newCorrelationId = cluster.aeron().nextCorrelationId();
    sendMessage(newCorrelationId, MessageIdentifier.JOB_EXPIRED);
  }

  private void sendMessage(long correlationId, MessageIdentifier identifier) {
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

    // client sends message
    while (sessionDecorator.offer(publication, correlationId, msgBuffer, 0, 4) < 0)
    {
      Thread.yield();
    }
  }

  private void sendMessage(Job job, long correlationId) {
    if (clientCluster == null)
    {
      clientCluster = AeronCluster.connect(
        new AeronCluster.Context().aeron(cluster.aeron()));
    }

    // write job created
    final Aeron aeron = clientCluster.context().aeron();
    final SessionDecorator sessionDecorator = new SessionDecorator(clientCluster.clusterSessionId());
    final Publication publication = clientCluster.ingressPublication();

    // client sends message
    while (sessionDecorator.offer(publication, correlationId, job.asBuffer(), 0, job.length()) < 0)
    {
      Thread.yield();
    }
  }


}
