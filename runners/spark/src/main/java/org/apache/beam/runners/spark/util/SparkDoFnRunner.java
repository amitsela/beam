package org.apache.beam.runners.spark.util;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.ExecutionContext.StepContext;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


/**
 * Created by ansela on 10/15/16.
 */
public class SparkDoFnRunner<InputT, OutputT> extends SimpleDoFnRunner<InputT, OutputT> {
  private static final Logger LOG = LoggerFactory.getLogger(SparkDoFnRunner.class);

  public SparkDoFnRunner(OldDoFn<InputT, OutputT> fn,
                         SparkRuntimeContext runtimeContext,
                         Accumulator<NamedAggregators> accum,
                         Map<TupleTag<?>, KV<WindowingStrategy<?, ?>,
                             BroadcastHelper<?>>> sideInputs,
                         TupleTag<OutputT> mainOutputTag,
                         List<TupleTag<?>> sideOutputTags,
                         WindowingStrategy<?, ?> windowingStrategy) {
    super(runtimeContext.getPipelineOptions(), fn, SparkSideInputReader.create(sideInputs),
        SparkOutputManager.create(), mainOutputTag, sideOutputTags, null,
            SparkAggregatorFactory.forAccumulator(runtimeContext, accum), windowingStrategy);
  }

  public Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>>
      processPartition(final Iterator<WindowedValue<InputT>> iter) {
        // skip if bundle is empty.
        if (!iter.hasNext()) {
          return Collections.emptyList();
        }
        // call start bundle.
        try {
          fn.setup();
          startBundle();
        } catch (Exception e) {
          // handle any exceptions thrown by DoFn#setup() or startBundle()
          // and handle the teardown of the DoFn.
          handleProcessingException(e);
          throw wrapUserCodeException(e);
        }
        return new Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>>() {
          @Override
          public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> iterator() {
            return new ProcessPartitionIterator(iter);
          }
        };
  }

  private class ProcessPartitionIterator
      extends AbstractIterator<Tuple2<TupleTag<?>, WindowedValue<?>>>{
    private final Iterator<WindowedValue<InputT>> inputIterator;
    private Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> outputIterator;
    private boolean calledFinish;

    ProcessPartitionIterator(Iterator<WindowedValue<InputT>> iter) {
      this.inputIterator = iter;
      this.outputIterator = ((SparkOutputManager) getOutputManager()).getOutputIterator();
    }

    @Override
    protected Tuple2<TupleTag<?>, WindowedValue<?>> computeNext() {
      // Process each element from the (input) iterator, which produces, zero, one or more
      // output elements (of type ValueT) in the output iterator. Note that the output
      // collection (and iterator) is reset between each call to processElement, so the
      // collection only holds the output values for each call to processElement, rather
      // than for the whole partition (which would use too much memory).
      while (true) {
        if (outputIterator.hasNext()) {
          return outputIterator.next();
        } else if (inputIterator.hasNext()) {
          ((SparkOutputManager) getOutputManager()).clear();
          try {
            processElement(inputIterator.next());
          } catch (Exception e) {
            // handle any exceptions thrown during processElement
            // and handle the teardown of the DoFn.
            handleProcessingException(e);
            throw e;
          }
          outputIterator = ((SparkOutputManager) getOutputManager()).getOutputIterator();
        } else {
          // no more input to consume, but finishBundle can produce more output
          if (!calledFinish) {
            ((SparkOutputManager) getOutputManager()).clear();
            try {
              calledFinish = true;
              finishBundle();
            } catch (Exception e) {
              // handle any exceptions thrown finishBundle()
              // and handle the teardown of the DoFn.
              handleProcessingException(e);
              throw e;
            }
            outputIterator = ((SparkOutputManager) getOutputManager()).getOutputIterator();
            continue; // try to consume outputIterator from start of loop
          }
          try {
            fn.teardown();
          } catch (Exception teardownException) {
            // avoid throwing an exception on fn teardown if processing of elements succeeded.
            LOG.error("Suppressing teardown exception that occurred after processing entire input",
                teardownException);
          }
          return endOfData();
        }
      }
    }
  }

  private void handleProcessingException(Exception e) {
    try {
      fn.teardown();
    } catch (Exception e1) {
      LOG.error("Exception while cleaning up DoFn", e1);
      e.addSuppressed(e1);
    }
  }

  private static class SparkOutputManager implements OutputManager {
    private final Multimap<TupleTag<?>, WindowedValue<?>> outputs = LinkedListMultimap.create();

    private SparkOutputManager() { }

    public static SparkOutputManager create() {
      return new SparkOutputManager();
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
    }

    public void clear() {
      outputs.clear();
    }

    Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> getOutputIterator() {
      return Iterators.transform(outputs.entries().iterator(),
          new Function<Map.Entry<TupleTag<?>, WindowedValue<?>>,
              Tuple2<TupleTag<?>, WindowedValue<?>>>() {
                @Override
                public Tuple2<TupleTag<?>, WindowedValue<?>>
                    apply(Map.Entry<TupleTag<?>, WindowedValue<?>> input) {
                      return new Tuple2<TupleTag<?>, WindowedValue<?>>(input.getKey(),
                          input.getValue());
                    }
              }
      );
    }
  }

  private static class SparkAggregatorFactory implements AggregatorFactory {
    private final SparkRuntimeContext runtimeContext;
    private final Accumulator<NamedAggregators> accumulator;

    private SparkAggregatorFactory(SparkRuntimeContext runtimeContext,
                                  Accumulator<NamedAggregators> accumulator) {
      this.runtimeContext = runtimeContext;
      this.accumulator = accumulator;
    }

    static SparkAggregatorFactory forAccumulator(SparkRuntimeContext runtimeContext,
                                                 Accumulator<NamedAggregators> accumulator) {
      return new SparkAggregatorFactory(runtimeContext, accumulator);
    }

    @Override
    public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT>
        createAggregatorForDoFn(Class<?> fnClass,
                                StepContext stepContext,
                                String aggregatorName,
                                Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
      return runtimeContext.createAggregator(accumulator, aggregatorName, combineFn);
    }
  }

}
