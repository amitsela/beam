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
import java.util.LinkedList;
import java.util.Queue;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;


/**
 * Create an input stream from Queue. For SparkRunner tests only.
 *
 * @param <T> stream type
 */
public final class CreateStream<T> extends PTransform<PBegin, PCollection<T>> {

  private final Queue<Iterable<T>> queuedValues = new LinkedList<>();

  private CreateStream(Iterable<T> first) {
    queuedValues.offer(first);
  }

  @SafeVarargs
  public static <T> CreateStream<T> withFirstBatch(T... batchValues) {
    return new CreateStream<>(Arrays.asList(batchValues));
  }

  public CreateStream<T> followedBy(T... batchValues) {
    queuedValues.offer(Arrays.asList(batchValues));
    return this;
  }

  public Queue<Iterable<T>> getQueuedValues() {
    return queuedValues;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return PCollection.createPrimitiveOutputInternal(
        input.getPipeline(), WindowingStrategy.globalDefault(), PCollection.IsBounded.UNBOUNDED);
  }
}
