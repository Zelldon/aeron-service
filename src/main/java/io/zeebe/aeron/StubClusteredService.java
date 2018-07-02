/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.aeron;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public class StubClusteredService implements ClusteredService
{
  protected Cluster cluster;

  public void onStart(final Cluster cluster)
  {
    System.out.println("Cluster starts ");
    System.out.println("Current role " + cluster.role());
    this.cluster = cluster;
  }

  public void onSessionOpen(final ClientSession session, final long timestampMs)
  {
    System.out.println("Session opened.");
  }

  public void onSessionClose(final ClientSession session, final long timestampMs, final CloseReason closeReason)
  {
    System.out.println();
    System.out.println("Session closed: " + closeReason);
    cluster.aeron().printCounters(System.out);

  }

  public void onSessionMessage(
    final long clusterSessionId,
    final long correlationId,
    final long timestampMs,
    final DirectBuffer buffer,
    final int offset,
    final int length,
    final Header header)
  {
  }

  public void onTimerEvent(final long correlationId, final long timestampMs)
  {
    System.out.println("Timer event.");
  }

  public void onTakeSnapshot(final Publication snapshotPublication)
  {
    System.out.println("Take snapshot.");
  }

  public void onLoadSnapshot(final Image snapshotImage)
  {
    System.out.println("Load snapshot.");
  }

  public void onReplayBegin()
  {
    System.out.println("Replay starts");
  }

  public void onReplayEnd()
  {
    System.out.println("Replay ends.");
  }

  public void onRoleChange(final Cluster.Role newRole)
  {
    System.out.println("Role changes to " + newRole);
  }

  public void onReady()
  {
    System.out.println("On Ready");
  }
}