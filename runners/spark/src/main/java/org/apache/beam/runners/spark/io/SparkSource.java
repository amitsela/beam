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

import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/***
 * Classes implementing Beam's {@link Source}.
 */
public class SparkSource {
  /**
   * A SourceRDD reads input from a {@link BoundedSource} and creates a Spark {@link RDD}.
   *
   * This is the basic way to read data from Beam's {@link Source}, but it's not a replacement for
   * optimized implementations such as a direct translation for {@link
   * org.apache.beam.sdk.io.TextIO} or even {@link org.apache.beam.sdk.transforms.Create.Values}.
   */
  public static class SourceRDD<T> extends RDD<WindowedValue<T>> {

    private final BoundedSource<T> boundedSource;
    private final SparkRuntimeContext runtimeContext;
    private final int numPartitions;

    private static final scala.collection.immutable.List<Dependency<?>> NIL =
        scala.collection.immutable.List.empty();

    public SourceRDD(SparkContext sc,
                     BoundedSource<T> boundedSource,
                     SparkRuntimeContext runtimeContext) {
      super(sc, NIL, scala.reflect.ClassTag$.MODULE$.<WindowedValue<T>>apply(WindowedValue.class));
      this.boundedSource = boundedSource;
      this.runtimeContext = runtimeContext;
      // the input parallelism is determined by Spark's scheduler backend.
      // when running on YARN/SparkDeploy it's the result of max(totalCores, 2).
      // when running on Mesos it's 8.
      // when running local it's the total number of cores (local = 1, local[N] = N,
      // local[*] = estimation of the machine's cores).
      // ** the configuration "spark.default.parallelism" takes precedence on all of the above **
      this.numPartitions = sc.defaultParallelism();
    }

    @Override
    public Partition[] getPartitions() {
      try {
        long desiredSizeBytes = boundedSource.getEstimatedSizeBytes(
            runtimeContext.getPipelineOptions()) / numPartitions;

        List<? extends Source<T>> partitionedSources =
            boundedSource.splitIntoBundles(desiredSizeBytes, runtimeContext.getPipelineOptions());
        Partition[] partitions = new SourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          partitions[i] = new SourcePartition<>(id(), i, partitionedSources.get(i));
        }
        return partitions;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create partitions.", e);
      }
    }

    @Override
    public scala.collection.Iterator<WindowedValue<T>>
    compute(final Partition split, TaskContext context) {
      final Iterator<WindowedValue<T>> iter;
      try {
        iter = new Iterator<WindowedValue<T>>() {
          @SuppressWarnings("unchecked")
          SourcePartition<T> partition = (SourcePartition<T>) split;
          BoundedSource.BoundedReader<T> reader = ((BoundedSource<T>) partition.source)
                  .createReader(runtimeContext.getPipelineOptions());
          boolean finished = !reader.start();
          boolean firstElement = true;

          @Override
          public boolean hasNext() {
            try {
              if (!finished) {
                // as long as there is data to read.
                if (!firstElement) {
                  // first element was advanced in the start() call so no need to advance again.
                  finished = !reader.advance();
                } else {
                  // this was the first element, but won't be anymore.
                  firstElement = false;
                }
                if (finished) {
                  // close the reader if there are no more elements to read.
                  reader.close();
                }
              }
              return !finished;
            } catch (IOException e) {
              throw new RuntimeException("Failed to read from reader.", e);
            }
          }

          @Override
          public WindowedValue<T> next() {
            return WindowedValue.of(reader.getCurrent(), reader.getCurrentTimestamp(),
                    GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
          }

          @Override
          public void remove() {
            // do nothing.
          }
        };
      } catch (IOException e) {
        throw new RuntimeException("Failed to create reader.", e);
      }

      return new InterruptibleIterator<>(context,
          scala.collection.JavaConversions.asScalaIterator(iter));
    }
  }

  /**
   * An input partition wrapping the sharded {@link Source}.
   */
  private static class SourcePartition<T> implements Partition {

    final int rddId;
    final int index;
    final Source<T> source;

    SourcePartition(int rddId, int index, Source<T> source) {
      this.rddId = rddId;
      this.index = index;
      this.source = source;
    }

    @Override
    public int index() {
      return index;
    }

    @Override
    public int hashCode() {
      return 31 * (31 + rddId) + index;
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }
  }
}
