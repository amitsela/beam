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
package org.apache.beam.runners.flink.translation.functions;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * A {@link RichGroupReduceFunction} for stateful {@link ParDo} in Flink Batch Runner.
 */
public class FlinkStatefulDoFnFunction<K, V, OutputT>
    extends RichGroupReduceFunction<WindowedValue<KV<K, V>>, WindowedValue<OutputT>> {

  private final DoFn<KV<K, V>, OutputT> dofn;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs;
  private final SerializedPipelineOptions serializedOptions;
  private final Map<TupleTag<?>, Integer> outputMap;
  private final TupleTag<OutputT> mainOutputTag;
  private transient DoFnInvoker doFnInvoker;

  public FlinkStatefulDoFnFunction(
      DoFn<KV<K, V>, OutputT> dofn,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> sideInputs,
      PipelineOptions pipelineOptions,
      Map<TupleTag<?>, Integer> outputMap,
      TupleTag<OutputT> mainOutputTag) {

    this.dofn = dofn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);
    this.outputMap = outputMap;
    this.mainOutputTag = mainOutputTag;
  }

  @Override
  public void reduce(
      Iterable<WindowedValue<KV<K, V>>> values,
      Collector<WindowedValue<OutputT>> out) throws Exception {
    RuntimeContext runtimeContext = getRuntimeContext();

    DoFnRunners.OutputManager outputManager;
    if (outputMap == null) {
      outputManager = new FlinkDoFnFunction.DoFnOutputManager(out);
    } else {
      // it has some sideOutputs
      outputManager =
          new FlinkDoFnFunction.MultiDoFnOutputManager((Collector) out, outputMap);
    }

    final Iterator<WindowedValue<KV<K, V>>> iterator = values.iterator();

    // get the first value, we need this for initializing the state internals with the key.
    // we are guaranteed to have a first value, otherwise reduce() would not have been called.
    WindowedValue<KV<K, V>> currentValue = iterator.next();
    final K key = currentValue.getValue().getKey();

    final InMemoryStateInternals<K> stateInternals = InMemoryStateInternals.forKey(key);
    DoFnRunner<KV<K, V>, OutputT> doFnRunner = DoFnRunners.simpleRunner(
        serializedOptions.getPipelineOptions(), dofn,
        new FlinkSideInputReader(sideInputs, runtimeContext),
        outputManager,
        mainOutputTag,
        // see SimpleDoFnRunner, just use it to limit number of side outputs
        Collections.<TupleTag<?>>emptyList(),
        new FlinkNoOpStepContext() {
          @Override
          public StateInternals<?> stateInternals() {
            return stateInternals;
          }
        },
        new FlinkAggregatorFactory(runtimeContext),
        windowingStrategy);

    doFnRunner.startBundle();

    doFnRunner.processElement(currentValue);
    while (iterator.hasNext()) {
      currentValue = iterator.next();
      doFnRunner.processElement(currentValue);
    }

    doFnRunner.finishBundle();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    doFnInvoker = DoFnInvokers.invokerFor(dofn);
    doFnInvoker.invokeSetup();
  }

  @Override
  public void close() throws Exception {
    doFnInvoker.invokeTeardown();
  }

}