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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.Instant;


/**
 * A {@link Broadcast} variable to hold the global watermarks for a microbatch.
 *
 * <p>Holds a queue for the watermarks of each microbatch that was read,
 * and advances the watermarks according to the queue (first-in-first-out).
 */
public class GlobalWatermarkHolder {
  private static final Instant START_TIME = new Instant(0);

  //TODO: broadcast a map of source-times to support multiple sources watermarks.
  private static volatile Broadcast<MicrobatchTime> broadcast = null;
  private static final Queue<MicrobatchTime> queue = new ConcurrentLinkedQueue<>();

  public static void add(MicrobatchTime microbatchTime) {
    queue.offer(microbatchTime);
  }

  public static void addAll(Queue<MicrobatchTime> microbatchTimes) {
    while (!microbatchTimes.isEmpty()) {
      add(microbatchTimes.poll());
    }
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
      if (queue.isEmpty()) {
        return;
      }
      Instant currentLowWatermark = START_TIME;
      Instant currentHighWatermark = START_TIME;
      Instant currentSynchronizedProcessingTime = START_TIME;
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
          String.format(
              "Low watermark %s cannot be later then high watermark %s",
              nextLowWatermark, nextHighWatermark));
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
    broadcast = null;
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

    @VisibleForTesting
    public MicrobatchTime(
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
