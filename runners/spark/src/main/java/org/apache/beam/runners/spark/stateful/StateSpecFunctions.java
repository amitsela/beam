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

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Table;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.GroupAlsoByWindowsDoFn;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.EmptyCheckpointMark;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.runners.spark.io.SparkUnboundedSource.Metadata;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.runners.spark.translation.TranslationUtils;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.LateDataUtils;
import org.apache.beam.runners.spark.util.UnsupportedSideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.dstream.PairDStreamFunctions;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction3;

/**
 * A class containing stateful transformations such as
 * {@link org.apache.spark.streaming.StateSpec} mappingFunctions,
 * {@link org.apache.spark.streaming.api.java.JavaPairDStream#updateStateByKey(Function2)},
 * and related work.
 */
public class StateSpecFunctions {
  private static final Logger LOG = LoggerFactory.getLogger(StateSpecFunctions.class);

  /**
   * A helper class that is essentially a {@link Serializable} {@link AbstractFunction1}.
   */
  private abstract static class SerializableFunction1<T1, T2>
      extends AbstractFunction1<T1, T2> implements Serializable {
  }

  public static <K, InputT, W extends BoundedWindow>
      JavaDStream<WindowedValue<KV<K, Iterable<InputT>>>> groupAlsoByWindow(
          JavaDStream<WindowedValue<KV<K, Iterable<WindowedValue<InputT>>>>> inputDStream,
          final Coder<InputT> iCoder,
          final WindowingStrategy<?, W> windowingStrategy,
          final SparkRuntimeContext runtimeContext) {
    // we have to move to Scala API to avoid Optional in the Java API.
    // see: SPARK-4819.
    // we also have a broader API for Scala (access to partitioner, etc.).
    DStream<Tuple2<K, Iterable<WindowedValue<InputT>>>> keyedInputDStream =
        inputDStream
            .map(WindowingHelpers.<KV<K, Iterable<WindowedValue<InputT>>>>unwindowFunction())
            .mapToPair(TranslationUtils.<K, Iterable<WindowedValue<InputT>>>toPairFunction())
            .dstream();

    PairDStreamFunctions<K, Iterable<WindowedValue<InputT>>> pairDStreamFunctions =
        DStream.toPairDStreamFunctions(
        keyedInputDStream,
        JavaSparkContext$.MODULE$.<K>fakeClassTag(),
        JavaSparkContext$.MODULE$.<Iterable<WindowedValue<InputT>>>fakeClassTag(),
        null);
    int defaultNumPartitions = pairDStreamFunctions.defaultPartitioner$default$1();
    Partitioner partitioner = pairDStreamFunctions.defaultPartitioner(defaultNumPartitions);

    //TODO: this is probably not resilient..
    JavaSparkContext jsc = new JavaSparkContext(pairDStreamFunctions.ssc().sc());
    final Accumulator<NamedAggregators> accumulator = AccumulatorSingleton.getInstance(jsc);

    DStream<Tuple2<K, Tuple2<StateAndTimers, List<WindowedValue<KV<K, Iterable<InputT>>>>>>>
        outputStateStream = pairDStreamFunctions
            .updateStateByKey(
                new SerializableFunction1<
                    /* generics look cumbersome here, but it's basically Iterator -> Iterator */
                    scala.collection.Iterator<
                        Tuple3<K, Seq<Iterable<WindowedValue<InputT>>>,
                        Option<Tuple2<StateAndTimers,
                        List<WindowedValue<KV<K, Iterable<InputT>>>>>>>>,
                    scala.collection.Iterator<Tuple2<K, Tuple2<StateAndTimers,
                        List<WindowedValue<KV<K, Iterable<InputT>>>>>>>>() {
      @Override
      public scala.collection.Iterator<Tuple2<K, Tuple2<StateAndTimers,
          List<WindowedValue<KV<K, Iterable<InputT>>>>>>> apply(
              final scala.collection.Iterator<Tuple3<K, Seq<Iterable<WindowedValue<InputT>>>,
              Option<Tuple2<StateAndTimers, List<WindowedValue<KV<K, Iterable<InputT>>>>>>>> iter) {
        //--- ACTUAL STATEFUL OPERATION:
        //
        // Input Iterator: the partition (~bundle) of a cogrouping of the input
        // and the previous state (if exists).
        //
        // Output Iterator: the output key, and the updated state.
        //
        // possible input scenarios for (K, Seq, Option<S>):
        // (1) Option<S>.isEmpty: new data with no previous state.
        // (2) Seq.isEmpty: no new data, but evaluating previous state (timer-like behaviour).
        // (3) Seq.nonEmpty && Option<S>.isDefined: new data with previous state.

        final SystemReduceFn<K, InputT, Iterable<InputT>, Iterable<InputT>, W> reduceFn =
            SystemReduceFn.buffering(iCoder);
        final OutputWindowedValueHolder<K, InputT> outputHolder =
            new OutputWindowedValueHolder<>();
        final Aggregator<Long, Long> droppedDueToClosedWindow = runtimeContext.createAggregator(
            accumulator,
            GroupAlsoByWindowsDoFn.DROPPED_DUE_TO_CLOSED_WINDOW_COUNTER,
            Sum.ofLongs());
        final Aggregator<Long, Long> droppedDueToLateness = runtimeContext.createAggregator(
            accumulator,
            GroupAlsoByWindowsDoFn.DROPPED_DUE_TO_LATENESS_COUNTER,
            Sum.ofLongs());

        AbstractIterator<
            Tuple2<K, Tuple2<StateAndTimers, List<WindowedValue<KV<K, Iterable<InputT>>>>>>>
                outIter = new AbstractIterator<Tuple2<K,
                    Tuple2<StateAndTimers, List<WindowedValue<KV<K, Iterable<InputT>>>>>>>() {
                  @Override
                  protected Tuple2<K, Tuple2<StateAndTimers,
                      List<WindowedValue<KV<K, Iterable<InputT>>>>>> computeNext() {
                    // input iterator is a Spark partition (~bundle), containing keys and their
                    // (possibly) previous-state and (possibly) new data.
                    while (iter.hasNext()) {
                      // for each element in the partition:
                      Tuple3<K, Seq<Iterable<WindowedValue<InputT>>>, Option<Tuple2<StateAndTimers,
                          List<WindowedValue<KV<K, Iterable<InputT>>>>>>> next = iter.next();
                      K key = next._1();

                      Seq<Iterable<WindowedValue<InputT>>> seq = next._2();

                      Option<Tuple2<StateAndTimers,
                          List<WindowedValue<KV<K, Iterable<InputT>>>>>>
                              prevStateAndTimersOpt = next._3();

                      SparkStateInternals<K> stateInternals;
                      SparkTimerInternals timerInternals = new SparkTimerInternals(
                          GlobalWatermarkHolder.get());
                      // get state(internals) per key.
                      if (prevStateAndTimersOpt.isEmpty()) {
                        // no previous state.
                        stateInternals = SparkStateInternals.forKey(key);
                      } else {
                        // with pre-existing state.
                        StateAndTimers prevStateAndTimers = prevStateAndTimersOpt.get()._1();
                        stateInternals = SparkStateInternals.forKeyAndState(key,
                            prevStateAndTimers.getState());
                        timerInternals.addTimers(prevStateAndTimers.getTimers());
                      }

                      ReduceFnRunner<K, InputT, Iterable<InputT>, W> reduceFnRunner =
                          new ReduceFnRunner<>(
                              key,
                              windowingStrategy,
                              ExecutableTriggerStateMachine.create(
                                  TriggerStateMachines.stateMachineForTrigger(
                                      windowingStrategy.getTrigger())),
                              stateInternals,
                              timerInternals,
                              outputHolder,
                              new UnsupportedSideInputReader("GroupAlsoByWindow"),
                              droppedDueToClosedWindow,
                              reduceFn,
                              runtimeContext.getPipelineOptions());

                      outputHolder.clear(); // clear before potential use.
                      if (!seq.isEmpty()) {
                        // new input for key.
                        try {
                          Iterable<WindowedValue<InputT>> elementsIterable = seq.head();
                          Iterable<WindowedValue<InputT>> validElements =
                              LateDataUtils
                                  .dropExpiredWindows(
                                      key,
                                      elementsIterable,
                                      timerInternals,
                                      windowingStrategy,
                                      droppedDueToLateness);
                          reduceFnRunner.processElements(validElements);
                        } catch (Exception e) {
                          throw new RuntimeException(
                              "Failed to process element with ReduceFnRunner", e);
                        }
                      } else if (stateInternals.getState().isEmpty()) {
                        // no input and no state -> GC evict now.
                        continue;
                      }
                      try {
                        // advance the watermark to HWM to fire by timers.
                        timerInternals.advanceWatermark();
                        // call on timers that are ready.
                        reduceFnRunner.onTimers(timerInternals.getTimersReadyToProcess());
                      } catch (Exception e) {
                        throw new RuntimeException(
                            "Failed to process ReduceFnRunner onTimer.", e);
                      }
                      // this is mostly symbolic since actual persist is done by emitting output.
                      reduceFnRunner.persist();
                      // obtain output, if fired.
                      List<WindowedValue<KV<K, Iterable<InputT>>>> outputs = outputHolder.get();
                      if (!outputs.isEmpty() || !stateInternals.getState().isEmpty()) {
                        StateAndTimers updated = new StateAndTimers(stateInternals.getState(),
                            timerInternals.getTimers());
                        // persist Spark's state by outputting.
                        return new Tuple2<>(key, new Tuple2<>(updated, outputs));
                      }
                      // an empty state with no output, can be evicted completely - do nothing.
                    }
                    return endOfData();
                  }
        };
        return scala.collection.JavaConversions.asScalaIterator(outIter);
      }
    }, partitioner, true,
        JavaSparkContext$.MODULE$.<Tuple2<StateAndTimers,
            List<WindowedValue<KV<K, Iterable<InputT>>>>>>fakeClassTag());
    // TODO: serialize typed data (K & InputT) for checkpointing ?
    // go back to Java now.
    // filter state-only output and remove state from output.
    return JavaPairDStream.fromPairDStream(outputStateStream,
        JavaSparkContext$.MODULE$.<K>fakeClassTag(),
        JavaSparkContext$.MODULE$.<Tuple2<StateAndTimers,
            List<WindowedValue<KV<K, Iterable<InputT>>>>>>fakeClassTag())
        .filter(
            new Function<Tuple2<K, Tuple2<StateAndTimers,
                List<WindowedValue<KV<K, Iterable<InputT>>>>>>, Boolean>() {
              @Override
              public Boolean call(
                  Tuple2<K, Tuple2<StateAndTimers,
                      List<WindowedValue<KV<K, Iterable<InputT>>>>>> t2) throws Exception {
                    // filter output if defined.
                return !t2._2()._2().isEmpty();
              }
        })
        .flatMap(new FlatMapFunction<Tuple2<K, Tuple2<StateAndTimers,
            List<WindowedValue<KV<K, Iterable<InputT>>>>>>,
                WindowedValue<KV<K, Iterable<InputT>>>>() {
              @Override
              public Iterable<WindowedValue<KV<K, Iterable<InputT>>>> call(
                  Tuple2<K, Tuple2<StateAndTimers,
                  List<WindowedValue<KV<K, Iterable<InputT>>>>>> t2) throws Exception {
                    // drop the state since it is already persisted at this point.
                return t2._2()._2();
              }
        });
  }

  private static class StateAndTimers {
    //Serializable state for internals (namespace to state tag to coded value).
    private final Table<String, String, byte[]> state;
    private final Collection<TimerInternals.TimerData> timers;

    private StateAndTimers(
        Table<String, String, byte[]> state, Collection<TimerInternals.TimerData> timers) {
      this.state = state;
      this.timers = timers;
    }

    public Table<String, String, byte[]> getState() {
      return state;
    }

    public Collection<TimerInternals.TimerData> getTimers() {
      return timers;
    }
  }

  private static class OutputWindowedValueHolder<K, V>
      implements OutputWindowedValue<KV<K, Iterable<V>>> {
    private List<WindowedValue<KV<K, Iterable<V>>>> windowedValues = new ArrayList<>();

    @Override
    public void outputWindowedValue(
        KV<K, Iterable<V>> output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      windowedValues.add(WindowedValue.of(output, timestamp, windows, pane));
    }

    private List<WindowedValue<KV<K, Iterable<V>>>> get() {
      return windowedValues;
    }

    private void clear() {
      windowedValues.clear();
    }

    @Override
    public <SideOutputT> void sideOutputWindowedValue(
        TupleTag<SideOutputT> tag,
        SideOutputT output, Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      throw new UnsupportedOperationException("Side outputs are not allowed in GroupAlsoByWindow.");
    }
  }

  /**
   * A helper class that is essentially a {@link Serializable} {@link AbstractFunction3}.
   */
  private abstract static class SerializableFunction3<T1, T2, T3, T4>
      extends AbstractFunction3<T1, T2, T3, T4> implements Serializable {
  }

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
   * And returns an iterator over all read values (for the micro-batch).
   *
   * <p>This stateful operation could be described as a flatMap over a single-element stream, which
   * outputs all the elements read from the {@link UnboundedSource} for this micro-batch.
   * Since micro-batches are bounded, the provided UnboundedSource is wrapped by a
   * {@link MicrobatchSource} that applies bounds in the form of duration and max records
   * (per micro-batch).
   *
   *
   * <p>In order to avoid using Spark Guava's classes which pollute the
   * classpath, we use the {@link StateSpec#function(scala.Function3)} signature which employs
   * scala's native {@link scala.Option}, instead of the
   * {@link StateSpec#function(org.apache.spark.api.java.function.Function3)} signature,
   * which employs Guava's {@link com.google.common.base.Optional}.
   *
   * <p>See also <a href="https://issues.apache.org/jira/browse/SPARK-4819">SPARK-4819</a>.</p>
   *
   * @param runtimeContext    A serializable {@link SparkRuntimeContext}.
   * @param <T>               The type of the input stream elements.
   * @param <CheckpointMarkT> The type of the {@link UnboundedSource.CheckpointMark}.
   * @return The appropriate {@link org.apache.spark.streaming.StateSpec} function.
   */
  public static <T, CheckpointMarkT extends UnboundedSource.CheckpointMark>
  scala.Function3<Source<T>, scala.Option<CheckpointMarkT>, State<Tuple2<byte[], Instant>>,
      Tuple2<Iterable<byte[]>, Metadata>> mapSourceFunction(
           final SparkRuntimeContext runtimeContext) {

    return new SerializableFunction3<
        Source<T>,
        Option<CheckpointMarkT>,
        State<Tuple2<byte[], Instant>>,
        Tuple2<Iterable<byte[]>, Metadata>>() {

      @Override
      public Tuple2<Iterable<byte[]>, Metadata> apply(
          Source<T> source,
          scala.Option<CheckpointMarkT> startCheckpointMark,
          State<Tuple2<byte[], Instant>> state) {
        // source as MicrobatchSource
        MicrobatchSource<T, CheckpointMarkT> microbatchSource =
            (MicrobatchSource<T, CheckpointMarkT>) source;

        // Initial high/low watermarks.
        Instant lowWatermark = new Instant(0);
        Instant highWatermark;

        // if state exists, use it, otherwise it's first time so use the startCheckpointMark.
        // startCheckpointMark may be EmptyCheckpointMark (the Spark Java API tries to apply
        // Optional(null)), which is handled by the UnboundedSource implementation.
        Coder<CheckpointMarkT> checkpointCoder = microbatchSource.getCheckpointMarkCoder();
        CheckpointMarkT checkpointMark;
        if (state.exists()) {
          // previous (output) watermark is now the low watermark.
          lowWatermark = state.get()._2();
          checkpointMark = CoderHelpers.fromByteArray(state.get()._1(), checkpointCoder);
          LOG.info("Continue reading from an existing CheckpointMark.");
        } else if (startCheckpointMark.isDefined()
            && !startCheckpointMark.get().equals(EmptyCheckpointMark.get())) {
          checkpointMark = startCheckpointMark.get();
          LOG.info("Start reading from a provided CheckpointMark.");
        } else {
          checkpointMark = null;
          LOG.info("No CheckpointMark provided, start reading from default.");
        }

        // create reader.
        BoundedSource.BoundedReader<T> reader;
        try {
          reader =
              microbatchSource.createReader(runtimeContext.getPipelineOptions(), checkpointMark);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        // read microbatch as a serialized collection.
        final List<byte[]> readValues = new ArrayList<>();
        WindowedValue.FullWindowedValueCoder<T> coder =
            WindowedValue.FullWindowedValueCoder.of(
                source.getDefaultOutputCoder(),
                GlobalWindow.Coder.INSTANCE);
        try {
          // measure how long a read takes per-partition.
          Stopwatch stopwatch = Stopwatch.createStarted();
          boolean finished = !reader.start();
          while (!finished) {
            WindowedValue<T> wv = WindowedValue.of(reader.getCurrent(),
                reader.getCurrentTimestamp(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
            readValues.add(CoderHelpers.toByteArray(wv, coder));
            finished = !reader.advance();
          }

          // end-of-read watermark is the high watermark.
          highWatermark = ((MicrobatchSource.Reader) reader).getWatermark();
          // close and checkpoint reader.
          reader.close();
          LOG.info("Source id {} spent {} msec on reading.", microbatchSource.getId(),
              stopwatch.stop().elapsed(TimeUnit.MILLISECONDS));

          // if the Source does not supply a CheckpointMark skip updating the checkpoint mark.
          @SuppressWarnings("unchecked")
          CheckpointMarkT finishedReadCheckpointMark =
              (CheckpointMarkT) ((MicrobatchSource.Reader) reader).getCheckpointMark();
          byte[] codedCheckpoint = new byte[0];
          if (finishedReadCheckpointMark != null) {
            codedCheckpoint = CoderHelpers.toByteArray(finishedReadCheckpointMark, checkpointCoder);
          } else {
            LOG.info("Skipping checkpoint marking because the reader failed to supply one.");
          }
          // persist the end-of-read (high) watermark for following read, where it will become
          // the next low watermark.
          state.update(new Tuple2<>(codedCheckpoint, highWatermark));
        } catch (IOException e) {
          throw new RuntimeException("Failed to read from reader.", e);
        }

        Iterable <byte[]> iterable = new Iterable<byte[]>() {
          @Override
          public Iterator<byte[]> iterator() {
            return Iterators.unmodifiableIterator(readValues.iterator());
          }
        };
        int numRecords = readValues.size();
        return new Tuple2<>(iterable, new Metadata(numRecords, lowWatermark, highWatermark));
      }
    };
  }
}
