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

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Classes implementing Beam {@link Source}s.
 */
public class SparkSource {

  //--- BOUNDED

  /**
   * A SourceRDD reads input from a {@link Source} and creates a Spark {@link RDD}.
   * This is the basic for the SparkRunner to read data from Beam BoundedSources.
   */
  public static class SourceRDD<T> extends RDD<WindowedValue<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(SourceRDD.class);

    private final Source<T> source;
    private final SparkRuntimeContext runtimeContext;
    private final int numPartitions;

    private static final scala.collection.immutable.List<Dependency<?>> NIL =
        scala.collection.immutable.List.empty();

    public SourceRDD(SparkContext sc,
                     Source<T> source,
                     SparkRuntimeContext runtimeContext) {
      super(sc, NIL, scala.reflect.ClassTag$.MODULE$.<WindowedValue<T>>apply(WindowedValue.class));
      this.source = source;
      this.runtimeContext = runtimeContext;
      // the input parallelism is determined by Spark's scheduler backend.
      // when running on YARN/SparkDeploy it's the result of max(totalCores, 2).
      // when running on Mesos it's 8.
      // when running local it's the total number of cores (local = 1, local[N] = N,
      // local[*] = estimation of the machine's cores).
      // ** the configuration "spark.default.parallelism" takes precedence over all of the above **
      this.numPartitions = sc.defaultParallelism();
    }

    private static final long DEFAULT_BUNDLE_SIZE = 64 * 1024 * 1024;

    @Override
    public Partition[] getPartitions() {
      long desiredSizeBytes = DEFAULT_BUNDLE_SIZE;
      try {
        desiredSizeBytes = ((BoundedSource<T>) source).getEstimatedSizeBytes(
            runtimeContext.getPipelineOptions()) / numPartitions;
      } catch (Exception e) {
        LOG.warn("Failed to get estimated size of bundle, default is " + DEFAULT_BUNDLE_SIZE
            + " bytes.");
      }
      try {
        List<? extends Source<T>> partitionedSources =
            ((BoundedSource<T>) source).splitIntoBundles(desiredSizeBytes,
                runtimeContext.getPipelineOptions());
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
      final Iterator<WindowedValue<T>> iter = new Iterator<WindowedValue<T>>() {
        @SuppressWarnings("unchecked")
        SourcePartition<T> partition = (SourcePartition<T>) split;
        BoundedSource.BoundedReader<T> reader = createReader(partition);

        private boolean finished = false;
        private boolean started = false;
        private boolean closed = false;

        @Override
        public boolean hasNext() {
          try {
            if (!started) {
              started = true;
              finished = !reader.start();
            } else {
              finished = !reader.advance();
            }
            if (finished) {
              // safely close the reader if there are no more elements left to read.
              closeIfNotClosed();
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

        private void closeIfNotClosed() {
          if (!closed) {
            closed = true;
            closeReader(reader);
          }
        }
      };

      return new InterruptibleIterator<>(context,
          scala.collection.JavaConversions.asScalaIterator(iter));
    }

    private BoundedSource.BoundedReader<T> createReader(SourcePartition<T> partition) {
      try {
        return ((BoundedSource<T>) partition.source).createReader(
            runtimeContext.getPipelineOptions());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create reader from a BoundedSource.", e);
      }
    }

    private void closeReader(BoundedSource.BoundedReader<T> reader) {
      try {
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close Reader.", e);
      }
    }
  }


  /**
   * An input {@link Partition} wrapping the sharded {@link Source}.
   */
  private static class SourcePartition<T> implements Partition {

    private final int rddId;
    private final int index;
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

  //--- UNBOUNDED

  /**
   * A CheckpointableSourceRDD creates a single-element-per-partition {@link RDD} containing the
   * {@link MicrobatchSource} and the (possibly) provided {@link UnboundedSource.CheckpointMark}
   * to start reading from.
   *
   * This will be followed by a
   * {@link org.apache.spark.streaming.dstream.PairDStreamFunctions#mapWithState} function that
   * will handle the {@link org.apache.beam.sdk.io.UnboundedSource.UnboundedReader} and manage the
   * CheckpointMarks.
   */
  public static class CheckpointableSourceRDD<T, CheckpointMarkT extends
      UnboundedSource.CheckpointMark> extends RDD<KV<Source<T>, CheckpointMarkT>> {
    private final MicrobatchSource<T, CheckpointMarkT> microbatchSource;
    private final SparkRuntimeContext runtimeContext;
    private final int numPartitions;

    private static final scala.collection.immutable.List<Dependency<?>> NIL =
            scala.collection.immutable.List.empty();

    //TODO: accept a list of CheckpointMarks to start from. This should determine
    // partitioning as well.
    public CheckpointableSourceRDD(SparkContext sc,
                                   UnboundedSource<T, CheckpointMarkT> source,
                                   SparkRuntimeContext runtimeContext,
                                   Duration duration) {
      super(sc, NIL, scala.reflect.ClassTag$.MODULE$.<KV<Source<T>,
          CheckpointMarkT>>apply(KV.class));
      this.runtimeContext = runtimeContext;
      this.numPartitions = sc.defaultParallelism();
      this.microbatchSource = new MicrobatchSource<>(source, duration, numPartitions, -1);
    }

    @Override
    public Partition[] getPartitions() {
      try {
        List<? extends BoundedSource<T>> partitionedSources = microbatchSource.splitIntoBundles(
            -1 /* ignored */, runtimeContext.getPipelineOptions());
        Partition[] partitions = new CheckpointableSourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          partitions[i] = new CheckpointableSourcePartition<>(id(), i, partitionedSources.get(i),
              EmptyCheckpointMark.get());
        }
        return partitions;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create partitions.", e);
      }
    }

    @Override
    public scala.collection.Iterator<KV<Source<T>, CheckpointMarkT>>
    compute(Partition split, TaskContext context) {
      @SuppressWarnings("unchecked")
      CheckpointableSourcePartition<T, CheckpointMarkT> partition =
          (CheckpointableSourcePartition<T, CheckpointMarkT>) split;
      KV<Source<T>, CheckpointMarkT> kv = KV.of(partition.source, partition.checkpointMark);
      return scala.collection.JavaConversions.asScalaIterator(Collections.singleton(kv).iterator());
    }
  }

  private static class CheckpointableSourcePartition<T, CheckpointMarkT extends
      UnboundedSource.CheckpointMark> extends SourcePartition<T> {
    private final CheckpointMarkT checkpointMark;

    CheckpointableSourcePartition(int rddId,
                                  int index,
                                  Source<T> source,
                                  CheckpointMarkT checkpointMark) {
      super(rddId, index, source);
      this.checkpointMark = checkpointMark;
    }
  }

  /** Passing null values to Spark's Java API may cause problems because of Guava preconditions.
   *  See: {@link org.apache.spark.api.java.JavaUtils#optionToOptional}
   * */
  public static class EmptyCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
    private static final EmptyCheckpointMark INSTANCE = new EmptyCheckpointMark();
    private static final int ID = 2654265; // a constant to serve as identifier.

    private EmptyCheckpointMark() {};

    public static EmptyCheckpointMark get() {
      return INSTANCE;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      // do nothing.
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof EmptyCheckpointMark;
    }

    @Override
    public int hashCode() {
      return ID;
    }
  }

  /**
   * A SourceDStream is a {@link org.apache.spark.streaming.dstream.DStream} of
   * {@link CheckpointableSourceRDD}s.
   */
  public static class SourceDStream<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      extends InputDStream<KV<Source<T>, CheckpointMarkT>> {

    private final UnboundedSource<T, CheckpointMarkT> unboundedSource;
    private final SparkRuntimeContext runtimeContext;
    private final boolean backPressureEnabled;

    public SourceDStream(StreamingContext ssc,
                         UnboundedSource<T, CheckpointMarkT> unboundedSource,
                         SparkRuntimeContext runtimeContext,
                         boolean backPressureEnabled) {
      super(ssc, scala.reflect.ClassTag$.MODULE$.<KV<Source<T>, CheckpointMarkT>>apply(KV.class));
      this.unboundedSource = unboundedSource;
      this.runtimeContext = runtimeContext;
      this.backPressureEnabled = backPressureEnabled;
    }

    @Override
    public scala.Option<RDD<KV<Source<T>, CheckpointMarkT>>> compute(Time validTime) {
      Duration batchDuration = new Duration(slideDuration().milliseconds());
      RDD<KV<Source<T>, CheckpointMarkT>> rdd = new CheckpointableSourceRDD<>(
          ssc().sc(), unboundedSource, runtimeContext, batchDuration);
      return scala.Option.apply(rdd);
    }

    //TODO: support back-pressure.

    @Override
    public void start() {
      // ignore.
    }

    @Override
    public void stop() {
      // ignore.
    }
  }
}
