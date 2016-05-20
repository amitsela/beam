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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import com.clearspring.analytics.util.Lists;

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.coders.EncoderHelpers;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.KVTupleFunctions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AssignWindowsDoFn;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.GroupedDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

/**
 * Supports translation between a Beam transform, and Spark's operations on Datasets.
 */
public class DatasetsTransformTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetsTransformTranslator.class);

  private DatasetsTransformTranslator() {
  }

  private static <K, V> TransformEvaluator<GroupByKeyOnly<K, V>> groupByKey() {
    return new TransformEvaluator<GroupByKeyOnly<K, V>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(GroupByKeyOnly<K, V> transform, EvaluationContext context) {
        DatasetsEvaluationContext dContext = (DatasetsEvaluationContext) context;
        Dataset<WindowedValue<KV<K, V>>> inputDataset =
            (Dataset<WindowedValue<KV<K, V>>>) dContext.getInputDataset(transform);

        //---- get coders
        KvCoder<K, V> kvCoder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        Coder<K> keyCoder = kvCoder.getKeyCoder();
        final Coder<V> valueCoder = kvCoder.getValueCoder();

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        GroupedDataset<ByteArray, Tuple2<ByteArray, byte[]>> grouped =
            inputDataset.map(WindowingHelpers.<KV<K, V>>unwindowFunctionD(),
            EncoderHelpers.<KV<K, V>>kryo())
            .map(KVTupleFunctions.<K, V>kVToTuple2(), EncoderHelpers.<K, V>tuple2Encoder())
            .map(CoderHelpers.toByteFunctionD(keyCoder, valueCoder),
            EncoderHelpers.<ByteArray, byte[]>tuple2Encoder())
            .groupBy(KVTupleFunctions.<ByteArray, byte[]>tuple2GetFirst(),
            EncoderHelpers.<ByteArray>kryo());

        // materialize grouped values - OOM hazard see GroupedDataset.mapGroups
        Dataset<Tuple2<ByteArray, Iterable<byte[]>>> materialized =
            grouped.mapGroups(new MapGroupsFunction<ByteArray, Tuple2<ByteArray, byte[]>,
            Tuple2<ByteArray, Iterable<byte[]>>>() {

          @Override
          public Tuple2<ByteArray, Iterable<byte[]>>
          call(ByteArray key, Iterator<Tuple2<ByteArray, byte[]>> values) throws Exception {
            List<byte[]> vs = Lists.newArrayList();
            while (values.hasNext()) {
              vs.add(values.next()._2());
            }
            return new Tuple2<>(key, Iterables.unmodifiableIterable(vs));
          }
        }, EncoderHelpers.<ByteArray, Iterable<byte[]>>tuple2Encoder());

        // decode bytes to the K, V they represent and wrap with a global window
        Dataset<WindowedValue<KV<K, Iterable<V>>>> outputDataset =
            materialized.map(CoderHelpers.fromByteFunctionIterableD(keyCoder, valueCoder),
            EncoderHelpers.<K, Iterable<V>>tuple2Encoder())
            .map(KVTupleFunctions.<K, Iterable<V>>tuple2ToKV(),
            EncoderHelpers.<KV<K, Iterable<V>>>kryo())
            .map(WindowingHelpers.<KV<K, Iterable<V>>>windowFunctionD(),
            EncoderHelpers.<WindowedValue<KV<K, Iterable<V>>>>kryo());

        dContext.setOutputDataset(transform, outputDataset);
      }
    };
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        PCollectionList<T> pCollectionList = context.getInput(transform);
        DatasetsEvaluationContext dContext = (DatasetsEvaluationContext) context;

        Dataset<WindowedValue<T>> flattened = dContext.getSQLContext()
            .emptyDataFrame().as(EncoderHelpers.<WindowedValue<T>>kryo());
        for (PCollection<T> pCollection: pCollectionList.getAll()) {
          flattened = flattened.union((Dataset<WindowedValue<T>>) dContext.getDataset(pCollection));
        }
        dContext.setOutputDataset(transform, flattened);
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.Bound<InputT, OutputT>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(ParDo.Bound<InputT, OutputT> transform, EvaluationContext context) {
        DatasetsEvaluationContext dContext = (DatasetsEvaluationContext) context;

        DoFnFunction<InputT, OutputT> doFn =
            new DoFnFunction<>(transform.getFn(),
                               dContext.getRuntimeContext(),
                               getSideInputs(transform.getSideInputs(), dContext));

        Dataset<WindowedValue<InputT>> dataset =
            (Dataset<WindowedValue<InputT>>) dContext.getInputDataset(transform);
        dContext.setOutputDataset(transform, dataset.mapPartitions(doFn,
            EncoderHelpers.<WindowedValue<OutputT>>kryo()));

      }
    };
  }


  private static <T> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {

      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        DatasetsEvaluationContext dContext = (DatasetsEvaluationContext) context;
        Dataset<WindowedValue<T>> inputDataset = (Dataset<WindowedValue<T>>)
            dContext.getInputDataset(transform);

        WindowFn<? super T, ?> windowFn = transform.getWindowFn();
        if (windowFn instanceof GlobalWindows) {
          ((DatasetsEvaluationContext) context).setOutputDataset(transform, inputDataset);
        } else {
          DoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          DoFnFunction<T, T> doFn =
              new DoFnFunction<>(addWindowsDoFn, dContext.getRuntimeContext(), null);
          dContext.setOutputDataset(transform,
              inputDataset.mapPartitions(doFn, EncoderHelpers.<WindowedValue<T>>kryo()));
        }
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> createValues() {
    return new TransformEvaluator<Create.Values<T>>() {

      @Override
      public void evaluate(Create.Values<T> transform, EvaluationContext context) {
        DatasetsEvaluationContext dContext = (DatasetsEvaluationContext) context;

        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = dContext.getOutput(transform).getCoder();
        dContext.setOutputDatasetFromValues(transform, transform.getElements(), coder);
      }
    };
  }

  private static TransformEvaluator<PTransform<?, ?>> view() {
    return new TransformEvaluator<PTransform<?, ?>>() {

      @Override
      public void evaluate(PTransform<?, ?> transform, EvaluationContext context) {
        DatasetsEvaluationContext dContext = (DatasetsEvaluationContext) context;
        Iterable<? extends WindowedValue<?>> iter =
                dContext.getWindowedValues((PCollection<?>) dContext.getInput(transform));
        dContext.setPView((PCollectionView<?>) dContext.getOutput(transform), iter);
      }
    };
  }
  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>>
      PRIMITIVES = Maps.newHashMap();

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>>
      EVALUATORS = Maps.newHashMap();

  static {
    //-------- SDK primitives
//    PRIMITIVES.put(Read.Bounded.class, readBounded());
//    PRIMITIVES.put(Read.Unbounded.class, readUnbounded());
    PRIMITIVES.put(GroupByKeyOnly.class, groupByKey());
    PRIMITIVES.put(Flatten.FlattenPCollectionList.class, flattenPColl());
    PRIMITIVES.put(ParDo.Bound.class, parDo());
    PRIMITIVES.put(Window.Bound.class, window());

    //-------- Composites
    EVALUATORS.put(Create.Values.class, createValues());
    EVALUATORS.put(View.AsSingleton.class, view());
    EVALUATORS.put(View.AsIterable.class, view());
    EVALUATORS.put(View.CreatePCollectionView.class, view());
//    EVALUATORS.put(Combine.GroupedValues.class, combineGrouped());
//    EVALUATORS.put(Combine.Globally.class, combineGlobally());
//    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
//    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());

  }

  private static Map<TupleTag<?>, BroadcastHelper<?>>
  getSideInputs(List<PCollectionView<?>> views, DatasetsEvaluationContext context) {
    if (views == null) {
      return ImmutableMap.of();
    } else {
      Map<TupleTag<?>, BroadcastHelper<?>> sideInputs = Maps.newHashMap();
      for (PCollectionView<?> view : views) {
        Iterable<? extends WindowedValue<?>> collectionView = context.getPCollectionView(view);
        Coder<Iterable<WindowedValue<?>>> coderInternal = view.getCoderInternal();
        @SuppressWarnings("unchecked")
        BroadcastHelper<?> helper =
            BroadcastHelper.create((Iterable<WindowedValue<?>>) collectionView, coderInternal);
        //broadcast side inputs
        helper.broadcast(context.getSparkContext());
        sideInputs.put(view.getTagInternal(), helper);
      }
      return sideInputs;
    }
  }

  /**
   * Translator to matches Beam transformations with the appropriate evaluator.
   */
  public static class Translator implements SparkPipelineTranslator {

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      // check if the PTransform has an evaluator or primitive support
      return EVALUATORS.containsKey(clazz) || PRIMITIVES.containsKey(clazz);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT> translate(
        Class<TransformT> clazz) {
      // try EVALUATORS
      LOG.debug("Look for a TransformEvaluator for transform {} in EVALUATORS registry",
          clazz.getSimpleName());
      TransformEvaluator<TransformT> transform =
          (TransformEvaluator<TransformT>) EVALUATORS.get(clazz);
      // try PRIMITIVES
      if (transform == null) {
        LOG.debug("Look for a TransformEvaluator for transform {} in PRIMITIVES registry",
            clazz.getSimpleName());
        transform = (TransformEvaluator<TransformT>) PRIMITIVES.get(clazz);
      }
      if (transform == null) {
        throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
      }
      return transform;
    }
  }
}
