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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import com.google.common.collect.Iterables;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.streaming.UnboundedDataset;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.Duration;


/**
 * Evaluation context allows us to define how pipeline instructions.
 */
public class EvaluationContext implements EvaluationResult {
  private final JavaSparkContext jsc;
  private final Pipeline pipeline;
  private final SparkRuntimeContext runtime;
  protected final Map<PValue, Dataset<?>> datasets = new LinkedHashMap<>();
  protected final Set<Dataset<?>> leaves = new LinkedHashSet<>();
  private final Set<PValue> multireads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private final Map<PValue, Iterable<? extends WindowedValue<?>>> pview = new LinkedHashMap<>();
  protected AppliedPTransform<?, ?, ?> currentTransform;

  public EvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.pipeline = pipeline;
    this.runtime = new SparkRuntimeContext(pipeline, jsc);
  }

  public JavaSparkContext getSparkContext() {
    return jsc;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public SparkRuntimeContext getRuntimeContext() {
    return runtime;
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  public <T extends PInput> T getInput(PTransform<T, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    T input = (T) currentTransform.getInput();
    return input;
  }

  public <T extends POutput> T getOutput(PTransform<?, T> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    T output = (T) currentTransform.getOutput();
    return output;
  }

  public <T> void checkinDataset(PTransform<?, ?> transform, Dataset<T> dataset) {
    checkinDataset((PValue) getOutput(transform), dataset);
  }

  public <T> void checkinDataset(PValue pvalue, Dataset<T> dataset) {
    try {
      dataset.setName(pvalue.getName());
    } catch (IllegalStateException e) {
      // name not set, ignore
    }
    datasets.put(pvalue, dataset);
    leaves.add(dataset);
  }

  public  <T> void setDatasetFromValues(PTransform<?, ?> transform, Iterable<T> values,
      Coder<T> coder) {
    WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
          WindowedValue.getValueOnlyCoder(coder);
      JavaRDD<WindowedValue<T>> rdd = jsc.parallelize(CoderHelpers.toByteArrays(
          Iterables.transform(values, WindowingHelpers.<T>windowValueFunction()), windowCoder))
              .map(CoderHelpers.fromByteFunction(windowCoder));
    datasets.put((PValue) getOutput(transform), new BoundedDataset<>(rdd));
  }

  public void setPView(PValue view, Iterable<? extends WindowedValue<?>> value) {
    pview.put(view, value);
  }

  public Dataset<?> checkoutDataset(PTransform<?, ?> transform) {
    return checkoutDataset((PValue) getInput(transform));
  }

  public Dataset<?> checkoutDataset(PValue pvalue) {
    Dataset<?> dataset = datasets.get(pvalue);
    leaves.remove(dataset);
    if (multireads.contains(pvalue)) {
      // Ensure the RDD is marked as cached
      dataset.cache();
    } else {
      multireads.add(pvalue);
    }
    return dataset;
  }

  <T> Iterable<? extends WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
    return pview.get(view);
  }

  public SparkPipelineTranslator getTranslator(TransformTreeNode transform) {

  }

  /**
   * Computes the outputs for all RDDs that are leaves in the DAG and do not have any
   * actions (like saving to a file) registered on them (i.e. they are performed for side
   * effects).
   */
  public void computeOutputs() {
    for (Dataset<?> dataset : leaves) {
      dataset.cache(); // cache so that any subsequent get() is cheap.
      dataset.action(); // force computation.
    }
  }

  @Override
  public <T> T get(PValue value) {
    //TODO
    //TODO
    //TODO
    //TODO
    //TODO
    //TODO
    throw new IllegalStateException("");
  }

  @Override
  public <T> T getAggregatorValue(String named, Class<T> resultType) {
    return runtime.getAggregatorValue(AccumulatorSingleton.getInstance(jsc), named, resultType);
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    return runtime.getAggregatorValues(AccumulatorSingleton.getInstance(jsc), aggregator);
  }

  @Override
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> boundedDataset = (BoundedDataset<T>) datasets.get(pcollection);
    Iterable<WindowedValue<T>> windowedValues = boundedDataset.getValues(pcollection);
    return Iterables.transform(windowedValues, WindowingHelpers.<T>unwindowValueFunction());
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    BoundedDataset<T> boundedDataset = (BoundedDataset<T>) datasets.get(pcollection);
    return boundedDataset.getValues(pcollection);
  }

  @Override
  public void close() {
    SparkContextFactory.stopSparkContext(jsc);
  }

  /** The runner is blocking. */
  @Override
  public State getState() {
    return State.DONE;
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException(
        "Spark runner EvaluationContext does not support cancel.");
  }

  @Override
  public State waitUntilFinish()
      throws IOException, InterruptedException {
    return waitUntilFinish(Duration.millis(-1));
  }

  @Override
  public State waitUntilFinish(Duration duration)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException(
        "Spark runner EvaluationContext does not support waitUntilFinish.");
  }
}
