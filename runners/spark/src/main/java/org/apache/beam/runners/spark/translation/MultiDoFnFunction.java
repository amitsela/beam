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

package org.apache.beam.runners.spark.translation;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.runners.spark.util.SparkDoFnRunner;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;



/**
 * DoFunctions ignore side outputs. MultiDoFunctions deal with side outputs by enriching the
 * underlying data with multiple TupleTags.
 *
 * @param <InputT> Input type for DoFunction.
 * @param <OutputT> Output type for DoFunction.
 */
public class MultiDoFnFunction<InputT, OutputT>
    implements PairFlatMapFunction<Iterator<WindowedValue<InputT>>, TupleTag<?>,
        WindowedValue<?>> {
  private final Accumulator<NamedAggregators> accum;
  private final OldDoFn<InputT, OutputT> fn;
  private final SparkRuntimeContext runtimeContext;
  private final TupleTag<OutputT> mainOutputTag;
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs;
  private final WindowingStrategy<?, ?> windowingStrategy;

  /**
   * @param accum             The Spark Accumulator that handles the Beam Aggregators.
   * @param fn                DoFunction to be wrapped.
   * @param runtimeContext    Runtime to apply function in.
   * @param mainOutputTag     The main output {@link TupleTag}.
   * @param sideInputs        Side inputs used in DoFunction.
   * @param windowingStrategy Input {@link WindowingStrategy}.
   */
  public MultiDoFnFunction(Accumulator<NamedAggregators> accum,
                           OldDoFn<InputT, OutputT> fn,
                           SparkRuntimeContext runtimeContext,
                           TupleTag<OutputT> mainOutputTag,
                           Map<TupleTag<?>, KV<WindowingStrategy<?, ?>,
                               BroadcastHelper<?>>> sideInputs,
                           WindowingStrategy<?, ?> windowingStrategy) {
    this.accum = accum;
    this.fn = fn;
    this.runtimeContext = runtimeContext;
    this.mainOutputTag = mainOutputTag;
    this.sideInputs = sideInputs;
    this.windowingStrategy = windowingStrategy;
  }

  @Override
  public Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>> call(Iterator<WindowedValue<InputT>> iter)
      throws Exception {
    SparkDoFnRunner<InputT, OutputT> doFnRunner = new SparkDoFnRunner<>(fn, runtimeContext, accum,
        sideInputs, mainOutputTag, Collections.<TupleTag<?>>emptyList(), windowingStrategy);
    return doFnRunner.processPartition(iter);
  }

}
