package org.apache.beam.runners.spark.io;

import com.google.api.client.util.BackOff;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.IntervalBoundedExponentialBackOff;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * Created by ansela on 8/20/16.
 */
public class MicrobatchSource {

  public static class AsBounded<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      extends BoundedSource<T> {

    private final UnboundedSource<T, CheckpointMarkT> source;
    private final long maxNumRecords;
    private final Duration maxReadTime;
    public final int id;

    public AsBounded(UnboundedSource<T, CheckpointMarkT> source,
                     long maxNumRecords,
                     Duration maxReadTime,
                     int id) {
      this.source = source;
      this.maxNumRecords = maxNumRecords;
      this.maxReadTime = maxReadTime;
      this.id = id;
    }

    /**
     * Divide the given number of records into {@code numSplits} approximately
     * equal parts that sum to {@code numRecords}.
     */
    private static long[] splitNumRecords(long numRecords, int numSplits) {
      long[] splitNumRecords = new long[numSplits];
      for (int i = 0; i < numSplits; i++) {
        splitNumRecords[i] = numRecords / numSplits;
      }
      for (int i = 0; i < numRecords % numSplits; i++) {
        splitNumRecords[i] = splitNumRecords[i] + 1;
      }
      return splitNumRecords;
    }

    /**
     * Pick a number of initial splits based on the number of records expected to be processed.
     */
    private static int numInitialSplits(long numRecords) {
      final int maxSplits = 100;
      final long recordsPerSplit = 10000;
      return (int) Math.min(maxSplits, numRecords / recordsPerSplit + 1);
    }

    @Override
    public List<? extends BoundedSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
                                                             PipelineOptions options)
        throws Exception {
      List<MicrobatchSource.AsBounded<T, CheckpointMarkT>> result = new ArrayList<>();
      int numInitialSplits = numInitialSplits(maxNumRecords);
      List<? extends UnboundedSource<T, CheckpointMarkT>> splits =
              source.generateInitialSplits(numInitialSplits, options);
      int numSplits = splits.size();
      long[] numRecords = splitNumRecords(maxNumRecords, numSplits);
      for (int i = 0; i < numSplits; i++) {
        result.add(new MicrobatchSource.AsBounded<>(splits.get(i), numRecords[i], maxReadTime, i));
      }
      return result;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      // No way to estimate bytes, so returning 0.
      return 0L;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return createReader(options, null);
    }

    public BoundedReader<T> createReader(PipelineOptions options, CheckpointMarkT checkpointMark)
        throws IOException {
      return new Reader(source.createReader(options, checkpointMark));
    }

    public UnboundedSource<T, CheckpointMarkT> getUnderlyingUnboundedSource() {
      return source;
    }

    @Override
    public void validate() {
      source.validate();
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return source.getDefaultOutputCoder();
    }

    public class Reader extends BoundedSource.BoundedReader<T> {
      private long recordsRead = 0L;
      private Instant endTime = Instant.now().plus(maxReadTime);
      private UnboundedSource.UnboundedReader<T> reader;

      private Reader(UnboundedSource.UnboundedReader<T> reader) {
        this.recordsRead = 0L;
        if (maxReadTime != null) {
          this.endTime = Instant.now().plus(maxReadTime);
        } else {
          this.endTime = null;
        }
        this.reader = reader;
      }

      @Override
      public boolean start() throws IOException {
        if (maxNumRecords <= 0 || (maxReadTime != null && maxReadTime.getMillis() == 0)) {
          return false;
        }

        if (reader.start()) {
          recordsRead++;
          return true;
        } else {
          return advanceWithBackoff();
        }
      }

      @Override
      public boolean advance() throws IOException {
        if (recordsRead >= maxNumRecords) {
          finalizeCheckpoint();
          return false;
        }
        return advanceWithBackoff();
      }

      private boolean advanceWithBackoff() throws IOException {
        // Try reading from the source with exponential backoff
        BackOff backoff = new IntervalBoundedExponentialBackOff(10000L, 10L);
        long nextSleep = backoff.nextBackOffMillis();
        while (nextSleep != BackOff.STOP) {
          if (endTime != null && Instant.now().isAfter(endTime)) {
            finalizeCheckpoint();
            return false;
          }
          if (reader.advance()) {
            recordsRead++;
            return true;
          }
          Uninterruptibles.sleepUninterruptibly(nextSleep, TimeUnit.MILLISECONDS);
          nextSleep = backoff.nextBackOffMillis();
        }
        finalizeCheckpoint();
        return false;
      }

      private void finalizeCheckpoint() throws IOException {
        reader.getCheckpointMark().finalizeCheckpoint();
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        return reader.getCurrent();
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return reader.getCurrentTimestamp();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public BoundedSource<T> getCurrentSource() {
        return AsBounded.this;
      }

      @SuppressWarnings("unchecked")
      public CheckpointMarkT getCheckpointMark() {
        return (CheckpointMarkT) reader.getCheckpointMark();
      }

      public long getRecordsRead() {
        return recordsRead;
      }
    }

  }

}
