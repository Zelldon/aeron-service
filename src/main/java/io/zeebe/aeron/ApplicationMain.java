package io.zeebe.aeron;

import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.zeebe.aeron.client.JobClient;
import io.zeebe.aeron.client.ZbClient;
import io.zeebe.aeron.service.JobService;

import java.io.File;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Scanner;

import org.agrona.CloseHelper;

public class ApplicationMain {

  private static final long MAX_CATALOG_ENTRIES = 1024;

  private static final int MEMBER_COUNT = 3;
  private static final String CLUSTER_MEMBERS = clusterMembersString();

  private static final String LOG_CHANNEL =
      "aeron:udp?term-length=64k|control-mode=manual|control=localhost:55550";
  private static final String ARCHIVE_CONTROL_REQUEST_CHANNEL =
      "aeron:udp?term-length=64k|endpoint=localhost:8010";
  private static final String ARCHIVE_CONTROL_RESPONSE_CHANNEL =
      "aeron:udp?term-length=64k|endpoint=localhost:8020";
  private static final int COUNT = 10_000;

  private static ClusteredMediaDriver[] clusteredMediaDrivers =
      new ClusteredMediaDriver[MEMBER_COUNT];
  private static ClusteredServiceContainer[] containers =
      new ClusteredServiceContainer[MEMBER_COUNT];

  public static void main(String args[]) throws InterruptedException {
    // init cluster
    init();

    Thread.sleep(1_000);
    
    // create client
    final ZbClient zbClient = ZbClient.newClient("localhost:20110,localhost:20111,localhost:20112");
    final JobClient jobClient = zbClient.jobClient();

    // subscribe
    jobClient.subscribe(
        (job -> {
          System.out.println("Received job on client.");
          jobClient.completeJob(job);
        }));

    // test
    new Thread(
            () -> {
              for (int i = 0; i < 5; i++) {
                jobClient.createJob();
              }
            })
        .start();

    waitUntilClose();

    zbClient.close();

    // tear down
    tearDown();
  }

  private static void init() {

    final String aeronDirName = CommonContext.getAeronDirectoryName();

    for (int i = 0; i < MEMBER_COUNT; i++) {

      final String baseDirName = aeronDirName + "-" + i;

      final AeronArchive.Context archiveCtx =
          new AeronArchive.Context()
              .controlRequestChannel(memberSpecificPort(ARCHIVE_CONTROL_REQUEST_CHANNEL, i))
              .controlRequestStreamId(100 + i)
              .controlResponseChannel(memberSpecificPort(ARCHIVE_CONTROL_RESPONSE_CHANNEL, i))
              .controlResponseStreamId(110 + i)
              .aeronDirectoryName(baseDirName);

      clusteredMediaDrivers[i] =
          ClusteredMediaDriver.launch(
              new MediaDriver.Context()
                  .aeronDirectoryName(baseDirName)
                  .threadingMode(ThreadingMode.SHARED)
                  .termBufferSparseFile(true)
                  .errorHandler(Throwable::printStackTrace)
                  .dirDeleteOnStart(true),
              new Archive.Context()
                  .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                  .aeronDirectoryName(baseDirName)
                  .archiveDir(new File(baseDirName, "archive"))
                  .controlChannel(archiveCtx.controlRequestChannel())
                  .controlStreamId(archiveCtx.controlRequestStreamId())
                  .localControlChannel("aeron:ipc?term-length=64k")
                  .localControlStreamId(archiveCtx.controlRequestStreamId())
                  .threadingMode(ArchiveThreadingMode.SHARED)
                  .deleteArchiveOnStart(true),
              new ConsensusModule.Context()
                  .clusterMemberId(i)
                  .clusterMembers(CLUSTER_MEMBERS)
                  .aeronDirectoryName(baseDirName)
                  .clusterDir(new File(baseDirName, "consensus-module"))
                  .ingressChannel("aeron:udp?term-length=64k")
                  .logChannel(memberSpecificPort(LOG_CHANNEL, i))
                  .archiveContext(archiveCtx.clone())
                  .errorHandler(Throwable::printStackTrace)
                  .deleteDirOnStart(true));

      containers[i] =
          ClusteredServiceContainer.launch(
              new ClusteredServiceContainer.Context()
                  .aeronDirectoryName(baseDirName)
                  .archiveContext(archiveCtx.clone())
                  .clusteredServiceDir(new File(baseDirName, "service"))
                  .clusteredService(new JobService())
                  .errorHandler(Throwable::printStackTrace)
                  .deleteDirOnStart(true));
    }
  }

  private static void tearDown() {
    //    CloseHelper.close(aeronCluster);
    //    CloseHelper.close(container);
    Arrays.stream(clusteredMediaDrivers).forEach(CloseHelper::close);

    Arrays.stream(clusteredMediaDrivers)
        .forEach(
            driver -> {
              CloseHelper.close(driver);
              driver.mediaDriver().context().deleteAeronDirectory();
            });
  }

  private static String memberSpecificPort(final String channel, final int memberId) {
    return channel.substring(0, channel.length() - 1) + memberId;
  }

  private static String clusterMembersString() {
    final StringBuilder builder = new StringBuilder();

    for (int i = 0; i < MEMBER_COUNT; i++) {
      builder
          .append(i) // id
          .append(',')
          .append("localhost:2011")
          .append(i) // client
          .append(',')
          .append("localhost:2022")
          .append(i) // member
          .append(',')
          .append("localhost:2033")
          .append(i) // log
          .append(',')
          .append("localhost:801")
          .append(i) // archive
          .append('|');
    }

    builder.setLength(builder.length() - 1);

    return builder.toString();
  }

  private static void waitUntilClose() {
    try (Scanner scanner = new Scanner(System.in)) {
      while (scanner.hasNextLine()) {
        final String nextLine = scanner.nextLine();
        if (nextLine.contains("close")) {
          return;
        }
      }
    }
  }
}
