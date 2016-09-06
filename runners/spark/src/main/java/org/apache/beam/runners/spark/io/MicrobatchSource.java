package org.apache.beam.runners.spark.io;

import com.google.api.client.util.BackOff;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.IntervalBoundedExponentialBackOff;
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * Mostly based on {@link org.apache.beam.sdk.io.BoundedReadFromUnboundedSource},
 * with some adjustments for this specific use-case.
 *
 * A {@link BoundedSource} wrapping an {@link UnboundedSource} to complement Spark's micro-batch
 * nature.
 *
 * By design, Spark's micro-batches are bounded by their duration. Spark also provides a
 * back-pressure mechanism that may signal a bound by max records.
 */
public class MicrobatchSource<T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    extends BoundedSource<T> {

  private final UnboundedSource<T, CheckpointMarkT> source;
  private final Duration maxReadTime;
  private final int numInitialSplits;
  private final int id;

  private long maxNumRecords = Long.MAX_VALUE;

  public MicrobatchSource(UnboundedSource<T, CheckpointMarkT> source,
                          Duration maxReadTime,
                          int numInitialSplits,
                          int id) {
    this.source = source;
    this.maxReadTime = maxReadTime;
    this.numInitialSplits = numInitialSplits;
    this.id = id;
  }

  public MicrobatchSource<T, CheckpointMarkT> withMaxRecords(long maxNumRecords) {
    this.maxNumRecords = maxNumRecords;
    return this;
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

  @Override
  public List<? extends BoundedSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
                                                           PipelineOptions options)
      throws Exception {
    List<MicrobatchSource<T, CheckpointMarkT>> result = new ArrayList<>();
    List<? extends UnboundedSource<T, CheckpointMarkT>> splits =
        source.generateInitialSplits(numInitialSplits, options);
    int numSplits = splits.size();
    long[] numRecords = splitNumRecords(maxNumRecords, numSplits);
    for (int i = 0; i < numSplits; i++) {
      //TODO: id is not a good enough identifier, for example if partitions are added.
      result.add(new MicrobatchSource<>(splits.get(i), maxReadTime, 1, i).withMaxRecords(
          numRecords[i]));
    }
    return result;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return 0;
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

  @Override
  public void validate() {
    source.validate();
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return source.getDefaultOutputCoder();
  }

  public Coder<CheckpointMarkT> getCheckpointMarkCoder() {
    return source.getCheckpointMarkCoder();
  }

  public int getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MicrobatchSource)) {
      return false;
    }
    MicrobatchSource<?, ?> that = (MicrobatchSource<?, ?>) o;

    return id == that.id;
  }

  @Override
  public int hashCode() {
    return id;
  }

  /**
   * A {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader}
   * wrapping an {@link org.apache.beam.sdk.io.UnboundedSource.UnboundedReader}.
   *
   * This Reader will read until it reached the bound of duration, or (if specified) max records,
   * whichever comes first.
   */
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
      // Backoff max interval cannot be greater then micro-batch duration
      long durationMillis = maxReadTime.getMillis();
      // Try reading from the source with exponential backoff
      BackOff backoff = new IntervalBoundedExponentialBackOff(durationMillis, 10L);
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
        long nextBackoff = backoff.nextBackOffMillis();
        long timeLeftMillis = endTime.getMillis() - Instant.now().getMillis();
        // if next backoff is larger then the time left for the micro-batch,
        // replace it with time left. We don't want to delay the micro-batch by waiting.
        nextSleep = nextBackoff <= timeLeftMillis ? nextBackoff : timeLeftMillis;
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
      return MicrobatchSource.this;
    }

    @SuppressWarnings("unchecked")
    public CheckpointMarkT getCheckpointMark() {
      return (CheckpointMarkT) reader.getCheckpointMark();
    }

    public long getNumRecordsRead() {
      return recordsRead;
    }
  }
}
