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

import java.text.SimpleDateFormat;
import java.util.Date;

public class StubClusteredService implements ClusteredService
{
  private static final String MESSAGE_FRAME = "[%s] [%s]: %s";
  private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("hh:mm:ss:SS");

  protected Cluster cluster;
  protected Cluster.Role currentRole;

  public void onStart(final Cluster cluster)
  {
	  this.cluster = cluster;
    logMessage(cluster.timeMs(), "Cluster starts ");
    logMessage(cluster.timeMs(), "Current role " + cluster.role());
  }

  public void onSessionOpen(final ClientSession session, final long timestampMs)
  {
    logMessage(timestampMs, "Opened session " + session.id());
  }

  public void onSessionClose(final ClientSession session, final long timestampMs, final CloseReason closeReason)
  {
    logMessage(timestampMs, "Closed session " + session.id() + " reason: " + closeReason);
//    cluster.aeron().printCounters(System.out);

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
    logMessage(cluster.timeMs(), "Take snapshot.");
  }

  public void onLoadSnapshot(final Image snapshotImage)
  {
    logMessage(cluster.timeMs(), "Load snapshot.");
  }

  public void onReplayBegin()
  {
    logMessage(cluster.timeMs(), "Replay starts");
  }

  public void onReplayEnd()
  {
    logMessage(cluster.timeMs(), "Replay ends.");
  }

  public void onRoleChange(final Cluster.Role newRole)
  {
    logMessage(cluster.timeMs(), "Role changes to " + newRole);
    currentRole = newRole;
  }

  public void onReady()
  {
    logMessage(cluster.timeMs(), "On Ready");
  }

  public void logMessage(long ms, String body)
  {
    System.out.println(String.format(MESSAGE_FRAME, DATE_FORMAT.format(new Date(ms)), cluster.aeron().context().aeronDirectoryName(), body));
  }

}