package org.apache.beam.runners.spark.stateful;

import com.google.common.base.Optional;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.io.SparkSource;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class containing {@link org.apache.spark.streaming.StateSpec} mappingFunctions.
 */
public class StateSpecFunctions {
  private static final Logger LOG = LoggerFactory.getLogger(StateSpecFunctions.class);

  /**
   * A {@link org.apache.spark.streaming.StateSpec} function to support reading from
   * an {@link UnboundedSource}.
   *
   * <p>This StateSpec function expects the following:
   * <ul>
   * <li>Key: The (partitioned) Source to read from.</li>
   * <li>Value: An optional {@link UnboundedSource.CheckpointMark} to start from.</li>
   * <li>State: A byte representation of the (previously) persisted CheckpointMark.</li>
   * </ul>
   * And returns an iterator over all read values (for the micro-batch).</p>
   *
   * <p>This stateful operation could be described as a flatMap over a single-element stream, which
   * outputs all the elements read from the {@link UnboundedSource} for this micro-batch.
   * Since micro-batches are bounded, the provided UnboundedSource is wrapped by a
   * {@link MicrobatchSource} that applies bounds in the form of duration and max records
   * (per micro-batch).</p>
   *
   * @param runtimeContext    A serializable {@link SparkRuntimeContext}.
   * @param <T>               The type of the input stream elements.
   * @param <CheckpointMarkT> The type of the {@link UnboundedSource.CheckpointMark}.
   * @return The appropriate {@link org.apache.spark.streaming.StateSpec} function.
   */
  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
      Function3<Source<T>, Optional<CheckpointMarkT>, /* CheckpointMarkT */State<byte[]>,
          Iterator<WindowedValue<T>>> mapSourceFunction(final SparkRuntimeContext runtimeContext) {

    return new Function3<Source<T>, Optional<CheckpointMarkT>, State<byte[]>,
        Iterator<WindowedValue<T>>>() {
      @Override
      public Iterator<WindowedValue<T>>
      call(Source<T> source,
           Optional<CheckpointMarkT> startCheckpointMark,
           State<byte[]> state) throws Exception {
        // source as MicrobatchSource
        MicrobatchSource<T, CheckpointMarkT> microbatchSource =
            (MicrobatchSource<T, CheckpointMarkT>) source;
        // if state exists, use it, otherwise it's first time so use the startCheckpointMark.
        // startCheckpointMark may be EmptyCheckpointMark (the Spark Java API tries to apply
        // Optional(null)), which is handled by the UnboundedSource implementation.
        Coder<CheckpointMarkT> checkpointCoder = microbatchSource.getCheckpointMarkCoder();
        CheckpointMarkT checkpointMark;
        if (state.exists()) {
          checkpointMark = CoderHelpers.fromByteArray(state.get(), checkpointCoder);
          LOG.info("Continue reading from an existing CheckpointMark.");
        } else if (startCheckpointMark.isPresent()
            && !startCheckpointMark.get().equals(SparkSource.EmptyCheckpointMark.get())) {
          checkpointMark = startCheckpointMark.get();
          LOG.info("Start reading from a provided CheckpointMark.");
        } else {
          checkpointMark = null;
          LOG.info("No CheckpointMark provided, start reading from default.");
        }
        // create reader.
        BoundedSource.BoundedReader<T> reader = microbatchSource.createReader(
            runtimeContext.getPipelineOptions(), checkpointMark);
        // read microbatch.
        final List<WindowedValue<T>> readValues = new ArrayList<>();
        try {
          boolean finished = !reader.start();
          while (!finished) {
            readValues.add(WindowedValue.of(reader.getCurrent(), reader.getCurrentTimestamp(),
                GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
            finished = !reader.advance();
          }
          // close and checkpoint reader.
          reader.close();
          // if no records were read, or the Source does not supply a CheckpointMark,
          // skip updating the state.
          @SuppressWarnings("unchecked")
          CheckpointMarkT finishedReadCheckpointMark = (CheckpointMarkT)
              ((MicrobatchSource.Reader) reader).getCheckpointMark();
          if (((MicrobatchSource.Reader) reader).getNumRecordsRead() > 0
              && finishedReadCheckpointMark != null) {
            state.update(CoderHelpers.toByteArray(finishedReadCheckpointMark, checkpointCoder));
          } else {
            LOG.info("Skipping checkpoint marking, either because no records were read "
                + "or because the reader failed to supply one.");
          }
        } catch (IOException e) {
          throw new RuntimeException("Failed to read from reader.", e);
        }
        return readValues.iterator();
      }
    };
  }
}
