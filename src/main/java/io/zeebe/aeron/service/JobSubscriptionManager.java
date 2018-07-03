package io.zeebe.aeron.service;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.SessionDecorator;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class JobSubscriptionManager extends Thread {

  private final List<Long> subscribedClients;
  private final Cluster cluster;
  private AeronCluster clientCluster;

  public JobSubscriptionManager(Cluster cluster) {
    this.cluster = cluster;
    subscribedClients = new ArrayList<>();
  }

  public void subscribeClient(long clientSessionId)
  {
    subscribedClients.add(clientSessionId);
  }

  public void assign(Job job) {
    final int nextRandom = ThreadLocalRandom.current().nextInt();
    final int index = nextRandom % subscribedClients.size();


    // get client via session
    final Long sessionId = subscribedClients.get(index);

    // assign job to client
    job.setClientSessionId(sessionId);

    // send assign message
    job.setMessageIdentifier(MessageIdentifier.JOB_ASSIGNED);

    long nextCorrelationId = cluster.aeron().nextCorrelationId();
    sendMessage(job, nextCorrelationId);
  }

  public void assigned(Job job)
  {
    System.out.println("Job assigned, sending to client.");

    final long clientSessionId = job.getClientSessionId();

    // get corresponding client
    final ClientSession clientSession = cluster.getClientSession(clientSessionId);

    // push assigned job to client
    final long nextCorrelationId = cluster.aeron().nextCorrelationId();
    clientSession.offer(nextCorrelationId, job.asBuffer(), 0, job.length());
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

//  private OneToOneConcurrentArrayQueue<Job> currentJobs = new OneToOneConcurrentArrayQueue<>(1024 * 64);
//
//  @Override
//  public void run() {
//    while (true)
//    {
//      Job job;
//      if ((job = currentJobs.poll()) != null)
//      {
//        assign(job);
//      }
//      else
//      {
//        Thread.yield();
//      }
//    }
//  }
}
