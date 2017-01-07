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
package org.apache.beam.runners.spark.stateful;

import javax.annotation.Nullable;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.Instant;


/**
 * An implementation of {@link TimerInternals} for the SparkRunner.
 */
class SparkTimerInternals implements TimerInternals {
  @Nullable private final Broadcast<Instant> watermarkBroadcast;

  SparkTimerInternals(@Nullable Broadcast<Instant> watermarkBroadcast) {
    this.watermarkBroadcast = watermarkBroadcast;
  }

  @Override
  public void setTimer(TimerData timer) {
    // do nothing.
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Deleting a timer by ID is not yet supported.");
  }

  @Override
  public void deleteTimer(TimerData timer) {
    // do nothing.
  }

  @Override
  public Instant currentProcessingTime() {
    return Instant.now();
  }

  @Nullable
  @Override
  public Instant currentSynchronizedProcessingTime() {
    //TODO: would be nice to use Spark's inner clock, maybe through batchTime ?
    return null;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return watermarkBroadcast != null ? watermarkBroadcast.getValue() : new Instant(Long.MIN_VALUE);
  }

  @Nullable
  @Override
  public Instant currentOutputWatermarkTime() {
    // TODO
    return null;
  }

  @Override
  public void setTimer(StateNamespace namespace,
                       String timerId,
                       Instant target,
                       TimeDomain timeDomain) {
    throw new UnsupportedOperationException("Setting a timer by ID not yet supported.");
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId) {
    throw new UnsupportedOperationException("Deleting a timer by ID is not yet supported.");
  }

}
