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

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.util.SerializableConfiguration;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/***
 * Classes implementing Beam's {@link Source}.
 */
public class SparkSource {

  //--- BOUNDED
  /**
   * A BoundedSourceRDD reads input from a {@link BoundedSource} and creates a Spark {@link RDD}.
   * This is the basic way to read data from Beam's BoundedSource.
   */
  public static class BoundedSourceRDD<T> extends RDD<WindowedValue<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedSourceRDD.class);

    protected final Source<T> source;
    protected final SparkRuntimeContext runtimeContext;
    protected final int numPartitions;

    private static final scala.collection.immutable.List<Dependency<?>> NIL =
        scala.collection.immutable.List.empty();

    public BoundedSourceRDD(SparkContext sc,
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
      // ** the configuration "spark.default.parallelism" takes precedence on all of the above **
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
        Partition[] partitions = new BoundedSourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          partitions[i] = new BoundedSourcePartition<>(id(), i, partitionedSources.get(i));
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
        iter = new Iterator<WindowedValue<T>>() {
          @SuppressWarnings("unchecked")
          BoundedSourcePartition<T> partition = (BoundedSourcePartition<T>) split;
          BoundedSource.BoundedReader<T> reader = createReader(partition);

          boolean finished = false;
          boolean started = false;
          boolean closed = false;

          @Override
          public boolean hasNext() {
            try {
              if (!started) {
                started = true;
                finished  = !reader.start();
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
            T current = reader.getCurrent();
            return WindowedValue.of(current, reader.getCurrentTimestamp(),
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

    protected BoundedSource.BoundedReader<T> createReader(BoundedSourcePartition<T> partition) {
      try {
        return ((BoundedSource<T>) partition.source).createReader(
                runtimeContext.getPipelineOptions());
      } catch (IOException e) {
        throw new RuntimeException("Failed to create reader from a BoundedSource.", e);
      }
    }

    protected void closeReader(BoundedSource.BoundedReader<T> reader) {
      try {
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close Reader.", e);
      }
    }
  }

  /**
   * An input partition wrapping the sharded {@link Source}.
   */
  public static class BoundedSourcePartition<T> implements Partition {

    protected final int rddId;
    protected final int index;
    protected final Source<T> source;

    BoundedSourcePartition(int rddId, int index, Source<T> source) {
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
   * An UnboundedSourceRDD reads input from an {@link UnboundedSource} and creates
   * a Spark {@link RDD} by using a Bounded-UnboundedSource with the help
   * of {@link MicrobatchSource}.
   * This is the basic way to read data from Beam's UnboundedSource.
   */
  public static class UnboundedSourceRDD<T,
      CheckpointMarkT extends UnboundedSource.CheckpointMark> extends BoundedSourceRDD<T> {

    private final Duration batchDuration;
    // metadata to handle checkpoints.
    private final Duration batchTime;
    private final String checkpointDirectory;
    private final BroadcastHelper<SerializableConfiguration> broadcastedHadoopConf;
    // if no limit is set, let the batchDuration use as bounds.
    private long maxNumRecords = Long.MAX_VALUE;

    public UnboundedSourceRDD(SparkContext sc,
                              Source<T> source,
                              SparkRuntimeContext runtimeContext,
                              Duration batchDuration,
                              Duration batchTime,
                              String checkpointDirectory,
                              BroadcastHelper<SerializableConfiguration> broadcastedHadoopConf) {
      super(sc, source, runtimeContext);
      this.batchDuration = batchDuration;
      this.batchTime = batchTime;
      this.checkpointDirectory = checkpointDirectory;
      this.broadcastedHadoopConf = broadcastedHadoopConf;
    }

    public UnboundedSourceRDD<T, CheckpointMarkT> withMaxNumRecords(long maxNumRecords) {
      UnboundedSourceRDD<T, CheckpointMarkT> rdd = new UnboundedSourceRDD<>(this.sparkContext(),
          this.source, this.runtimeContext, this.batchDuration, this.batchTime,
              this.checkpointDirectory, this.broadcastedHadoopConf);
      rdd.maxNumRecords = maxNumRecords;
      return rdd;
    }

    @Override
    public Partition[] getPartitions() {
      Duration prevBatchTime = batchTime.minus(batchDuration);
      String prevCheckpointDir = checkpointDirectory + "/" + prevBatchTime.getMillis();
      try {
        Duration boundDuration = new Duration((long) (batchDuration.getMillis() * 0.9));
        System.out.println(">>> Bounded duration is set to: " + boundDuration);
        // create a bounded-unbounded-source.
        MicrobatchSource.AsBounded<T, CheckpointMarkT> microbatchSource =
            new MicrobatchSource.AsBounded<>(((UnboundedSource<T, CheckpointMarkT>) source),
                maxNumRecords, boundDuration, -1 /* initial single-source id */);
        // get source's checkpointmark coder.
        Coder<CheckpointMarkT> coder =
                microbatchSource.getUnderlyingUnboundedSource().getCheckpointMarkCoder();
        // shard sources.
        List<? extends BoundedSource<T>> partitionedSources =
            microbatchSource.splitIntoBundles(-1 /* ignored */,
                runtimeContext.getPipelineOptions());
        // create the corresponding Spark partitions.
        Partition[] partitions =
            new SparkSource.UnboundedSourcePartition[partitionedSources.size()];
        for (int i = 0; i < partitionedSources.size(); i++) {
          // the checkpoint-path to the Partition.
          partitions[i] = new SparkSource.UnboundedSourcePartition<>(id(), i,
              partitionedSources.get(i), readCheckpointMark(coder, prevCheckpointDir + "/" + i));
        }
        return partitions;
      } catch (Exception e) {
        throw new RuntimeException("Failed to create partitions.", e);
      }
    }

    @Override
    protected BoundedSource.BoundedReader<T> createReader(BoundedSourcePartition<T> partition) {
      UnboundedSourcePartition<T, CheckpointMarkT> unboundedSourcePartition =
          (UnboundedSourcePartition<T, CheckpointMarkT>) partition;
      CheckpointMarkT checkpointMark = unboundedSourcePartition.checkpointMark;
      try {
        return ((MicrobatchSource.AsBounded<T, CheckpointMarkT>)
            unboundedSourcePartition.source).createReader(runtimeContext.getPipelineOptions(),
                checkpointMark);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create Reader from UnboundedSource.", e);
      }
    }

    @Override
    protected void closeReader(BoundedSource.BoundedReader<T> reader) {
      super.closeReader(reader);
      MicrobatchSource.AsBounded<T, CheckpointMarkT>.Reader unboundedBoundedReader =
          (MicrobatchSource.AsBounded<T, CheckpointMarkT>.Reader) reader;
      CheckpointMarkT checkpointMark =  unboundedBoundedReader.getCheckpointMark();
      MicrobatchSource.AsBounded<T, CheckpointMarkT> underlyingUnboundedSource =
          (MicrobatchSource.AsBounded<T, CheckpointMarkT>)
              unboundedBoundedReader.getCurrentSource();
      Coder<CheckpointMarkT> coder = ((MicrobatchSource.AsBounded<T, CheckpointMarkT>)
          unboundedBoundedReader.getCurrentSource()).getUnderlyingUnboundedSource()
              .getCheckpointMarkCoder();
      writeCheckpointMark(checkpointMark, coder, checkpointDirectory + "/"
          + batchTime.getMillis() + "/" + underlyingUnboundedSource.id);
    }

    private void writeCheckpointMark(CheckpointMarkT checkpointMark,
                                     Coder<CheckpointMarkT> coder,
                                     String destinationPath) {
      byte[] encodedCheckpointMark = CoderHelpers.toByteArray(checkpointMark, coder);
      long startTimer = System.nanoTime();
      Path path = new Path(destinationPath);
      Path tmpPath = new Path(path.getParent(), path.getName() + ".tmp");
      try {
        // write to a temporary file.
        long start1 = System.nanoTime();
        FileSystem fs = FileSystem.get(broadcastedHadoopConf.getValue().value());
        System.out.println(">>> getFS: "
                + ((System.nanoTime() - start1) / 1000000));
        long start2 = System.nanoTime();
        FSDataOutputStream os = fs.create(tmpPath);
        System.out.println(">>> create: "
                + ((System.nanoTime() - start2) / 1000000));

        os.write(encodedCheckpointMark);
        os.close();
        // finalize the write with rename.
        if (fs.exists(path) && !fs.delete(path, true)) {
          throw new RuntimeException("Failed to delete a pre-existing CheckpointMark at " + path);
        }
        if (!fs.rename(tmpPath, path)) {
          throw new RuntimeException("Failed to finalize CheckpointMark write.");
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to write CheckpointMark to " + destinationPath, e);
      }
      System.out.println(">>> Checkpoint write time was: "
          + ((System.nanoTime() - startTimer) / 1000000));
    }

    private CheckpointMarkT readCheckpointMark(Coder<CheckpointMarkT> coder,
                                               String sourcePath) {
      long startTimer = System.nanoTime();
      Path path = new Path(sourcePath);
      try {
        FileSystem fs = path.getFileSystem(broadcastedHadoopConf.getValue().value());
        if (!fs.exists(path)) {
          return null;
        }
        long fileSizeBytes = fs.getFileStatus(path).getLen();
        // assuming that the actual length is int size (or less)
        // because it was written as coded as bytes upon write.
        byte[] buffer = new byte[(int) fileSizeBytes];
        FSDataInputStream is = fs.open(path);
        is.readFully(0, buffer);
        is.close();
        System.out.println(">>> Checkpoint read time was: "
            + ((System.nanoTime() - startTimer) / 1000000));
        return CoderHelpers.fromByteArray(buffer, coder);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read CheckpointMark from " + sourcePath, e);
      }
    }
  }

  /**
   * An input partition wrapping the sharded {@link Source}
   * that uses {@link UnboundedSource.CheckpointMark}.
   */
  public static class UnboundedSourcePartition<T,
      CheckpointMarkT extends UnboundedSource.CheckpointMark> extends BoundedSourcePartition<T> {

    protected final CheckpointMarkT checkpointMark;

    UnboundedSourcePartition(int rddId,
                             int index,
                             Source<T> source,
                             CheckpointMarkT checkpointMark) {
      super(rddId, index, source);
      this.checkpointMark = checkpointMark;
    }
  }

  public static class SourceDStream<T, CheckpointMarkT
      extends UnboundedSource.CheckpointMark> extends InputDStream<WindowedValue<T>> {

    private final UnboundedSource<T, CheckpointMarkT> unboundedSource;
    private final SparkRuntimeContext runtimeContext;
    private final String checkpointDirectory;
    private final BroadcastHelper<SerializableConfiguration> broadcastedHadoopConf;
    private final boolean backPressureEnabled;

    public SourceDStream(StreamingContext ssc,
                         UnboundedSource<T, CheckpointMarkT> unboundedSource,
                         SparkRuntimeContext runtimeContext,
                         String checkpointDirectory,
                         BroadcastHelper<SerializableConfiguration> broadcastedHadoopConf,
                         boolean backPressureEnabled) {
      super(ssc, scala.reflect.ClassTag$.MODULE$.<WindowedValue<T>>apply(WindowedValue.class));
      this.unboundedSource = unboundedSource;
      this.runtimeContext = runtimeContext;
      this.checkpointDirectory = checkpointDirectory;
      this.broadcastedHadoopConf = broadcastedHadoopConf;
      this.backPressureEnabled = backPressureEnabled;
    }

    @Override
    public scala.Option<RDD<WindowedValue<T>>> compute(Time validTime) {
      Duration batchDuration = new Duration(slideDuration().milliseconds());
      Duration batchTime = new Duration(validTime.milliseconds());
      RDD<WindowedValue<T>> rdd = new UnboundedSourceRDD<T, CheckpointMarkT>(ssc().sc(),
          unboundedSource, runtimeContext, batchDuration, batchTime, checkpointDirectory,
              broadcastedHadoopConf);
      return scala.Option.apply(rdd);
    }

//    @Override
//    public scala.Option<RateController> rateController() {
//      if (backPressureEnabled) {
//
//      } else {
//        return scala.Option.empty();
//      }
//    }

    @Override
    public void start() {
      // ignore
    }

    @Override
    public void stop() {
      // ignore
    }
  }

}
