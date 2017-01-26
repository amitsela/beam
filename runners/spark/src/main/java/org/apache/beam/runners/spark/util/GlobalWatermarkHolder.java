/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.spark.util;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Time;
import org.joda.time.Instant;


/**
 * A {@link Broadcast} variable to hold the global watermarks for a microbatch.
 *
 * <p>Holds a queue for the watermarks of each microbatch that was read,
 * and advances the watermarks according to the queue (first-in-first-out).
 */
public class GlobalWatermarkHolder {

  //TODO: broadcast a map of source-times to support multiple sources watermarks.
  private static volatile Broadcast<MicrobatchTime> broadcast = null;
  private static final Queue<MicrobatchTime> queue = new ConcurrentLinkedQueue<>();

  public static void add(
      int sourceId,
      Instant lowWatermark,
      Instant highWatermark,
      Time processingTime) {
    Instant syncTime = new Instant(processingTime.milliseconds());
    queue.offer(new MicrobatchTime(sourceId, lowWatermark, highWatermark, syncTime));
  }

  /**
   * Returns the underlying {@link Broadcast} containing the global watermarks.
   */
  public static Broadcast<MicrobatchTime> get() {
    return broadcast;
  }

  /**
   * Advances the watermarks to the next-in-line watermarks.
   * Watermarks are monotonically increasing.
   */
  public static void advance(JavaSparkContext jsc) {
    synchronized (GlobalWatermarkHolder.class){
      Instant currentLowWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
      Instant currentHighWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
      Instant currentSynchronizedProcessingTime = BoundedWindow.TIMESTAMP_MIN_VALUE;
      if (broadcast != null) {
        currentLowWatermark = broadcast.getValue().getLowWatermark();
        currentHighWatermark = broadcast.getValue().getHighWatermark();
        currentSynchronizedProcessingTime = broadcast.getValue().getSynchronizedProcessingTime();
        broadcast.unpersist(true);
      }
      MicrobatchTime next = queue.poll();
      // watermarks advance monotonically.
      Instant nextLowWatermark = next.getLowWatermark().isAfter(currentLowWatermark)
          ? next.getLowWatermark() : currentLowWatermark;
      Instant nextHighWatermark = next.getHighWatermark().isAfter(currentHighWatermark)
          ? next.getHighWatermark() : currentHighWatermark;
      Instant nextSynchronizedProcessingTime = next.getSynchronizedProcessingTime();
      checkState(!nextLowWatermark.isAfter(nextHighWatermark),
          "Low watermark cannot be later then high watermark.");
      checkState(nextSynchronizedProcessingTime.isAfter(currentSynchronizedProcessingTime),
          "Synchronized processing time must advance.");
      broadcast = jsc.broadcast(
          new MicrobatchTime(
              next.getSourceId(),
              nextLowWatermark,
              nextHighWatermark,
              nextSynchronizedProcessingTime));
    }
  }

  @VisibleForTesting
  public static synchronized void clear() {
    queue.clear();
    if (broadcast != null) {
      broadcast.destroy(true);
      broadcast = null;
    }
  }

  /**
   * A {@link MicrobatchTime} holds the watermarks and processing time
   * relevant to a microbatch input from a specific source.
   */
  public static class MicrobatchTime implements Serializable {
    private final int sourceId;
    private final Instant lowWatermark;
    private final Instant highWatermark;
    private final Instant synchronizedProcessingTime;

    MicrobatchTime(
        int sourceId,
        Instant lowWatermark,
        Instant highWatermark,
        Instant synchronizedProcessingTime) {
      this.sourceId = sourceId;
      this.lowWatermark = lowWatermark;
      this.highWatermark = highWatermark;
      this.synchronizedProcessingTime = synchronizedProcessingTime;
    }

    public int getSourceId() {
      return sourceId;
    }

    public Instant getLowWatermark() {
      return lowWatermark;
    }

    public Instant getHighWatermark() {
      return highWatermark;
    }

    public Instant getSynchronizedProcessingTime() {
      return synchronizedProcessingTime;
    }
  }
}