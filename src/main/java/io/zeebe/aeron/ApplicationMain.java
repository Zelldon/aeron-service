package io.zeebe.aeron;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.zeebe.aeron.client.JobClient;
import io.zeebe.aeron.client.ZbClient;
import io.zeebe.aeron.service.JobService;
import org.agrona.CloseHelper;

public class ApplicationMain {

    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final int COUNT = 10_000;

    private static ClusteredMediaDriver clusteredMediaDriver;

    public static void main(String args[])
    {
      // init cluster
      init();

      // create client
      final JobClient jobClient = ZbClient.newClient().jobClient();

      // subscribe
      jobClient.subscribe((job -> {
        System.out.println("Received job on client.");
        jobClient.completeJob(job);
      }));


      // test
      new Thread(() ->
      {
        for (int i = 0; i < COUNT; i++)
        {
          jobClient.createJob();
        }
      }).start();


      while (true);

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

    ClusteredServiceContainer.launch(
      new ClusteredServiceContainer.Context()
        .clusteredService(new JobService())
        .errorHandler(Throwable::printStackTrace)
        .deleteDirOnStart(true));
  }

  private static void tearDown() {
//    CloseHelper.close(aeronCluster);
//    CloseHelper.close(container);
    CloseHelper.close(clusteredMediaDriver);

    if (null != clusteredMediaDriver)
    {
      clusteredMediaDriver.consensusModule().context().deleteDirectory();
      clusteredMediaDriver.archive().context().deleteArchiveDirectory();
      clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();
    }
  }

}
