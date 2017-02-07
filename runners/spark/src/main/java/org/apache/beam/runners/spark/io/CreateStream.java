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
package org.apache.beam.runners.spark.io;

import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.MicrobatchTime;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * Create an input stream from Queue. For SparkRunner tests only.
 *
 * @param <T> stream type.
 */
public final class CreateStream<T> extends PTransform<PBegin, PCollection<T>> {

  private final Duration batchInterval;
  private final Instant initialSystemTime;
  private final Queue<Iterable<T>> batches = new LinkedList<>();
  private final Deque<MicrobatchTime> times = new LinkedList<>();

  private Instant lowWatermark = new Instant(0);

  private CreateStream(Duration batchInterval, Instant initialSystemTime) {
    this.batchInterval = batchInterval;
    this.initialSystemTime = initialSystemTime;
  }

  /** Set the batch interval for the stream. */
  public static <T> CreateStream<T> withBatchInterval(Duration batchInterval) {
    return new CreateStream<>(batchInterval, new Instant(0));
  }

  /** Set the initial wall time. */
  public CreateStream<T> initialSystemTimeAt(Instant initialSystemTime) {
    return new CreateStream<>(batchInterval, initialSystemTime);
  }

  /**
   * Enqueue next micro-batch values.
   * This is backed by a {@link Queue} so stream input order would keep the population order (FIFO).
   */
  public CreateStream<T> nextBatch(T... batchValues) {
    batches.offer(Arrays.asList(batchValues));
    return this;
  }

  /**
   * Advances the watermark in the next batch.
   */
  public CreateStream<T> advanceWatermarkForNextBatch(Instant watermark) {
    // move the system time.
    Instant nextSynchronizedProcessingTime = times.peekLast() == null ? initialSystemTime
        : times.peekLast().getSynchronizedProcessingTime().plus(batchInterval);
    times.offer(new MicrobatchTime(-1, lowWatermark, watermark, nextSynchronizedProcessingTime));
    lowWatermark = watermark;
    return this;
  }

  /**
   * Advances the watermark in the next batch to the end-of-time.
   */
  public CreateStream<T> advanceNextBatchWatermarkToInfinity() {
    return advanceWatermarkForNextBatch(BoundedWindow.TIMESTAMP_MAX_VALUE);
  }

  /** Get the underlying queue representing the mock stream of micro-batches. */
  public Queue<Iterable<T>> getBatches() {
    return batches;
  }

  /**
   * Get times so they can be pushed into the
   * {@link org.apache.beam.runners.spark.util.GlobalWatermarkHolder}.
   */
  public Queue<MicrobatchTime> getTimes() {
    return times;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
  }

}
