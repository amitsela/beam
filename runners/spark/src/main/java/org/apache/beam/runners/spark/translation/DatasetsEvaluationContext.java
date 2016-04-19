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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AggregatorRetrievalException;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Evaluation context allows us to define how pipeline instructions.
 */
public class DatasetsEvaluationContext implements EvaluationResult {
  private final JavaSparkContext jsc;
  private final SQLContext sqlContext;
  private final Pipeline pipeline;
  private final SparkRuntimeContext runtime;
  private final Map<PValue, DatasetHolder<?>> pcollections = new LinkedHashMap<>();
  private final Set<DatasetHolder<?>> leafDatasets = new LinkedHashSet<>();
  private final Set<PValue> multireads = new LinkedHashSet<>();
  private final Map<PValue, Object> pobjects = new LinkedHashMap<>();
  private final Map<PValue, Iterable<? extends WindowedValue<?>>> pview = new LinkedHashMap<>();
  protected AppliedPTransform<?, ?, ?> currentTransform;

  public DatasetsEvaluationContext(JavaSparkContext jsc, Pipeline pipeline) {
    this.jsc = jsc;
    this.sqlContext = new SQLContext(jsc);
    this.pipeline = pipeline;
    this.runtime = new SparkRuntimeContext(jsc, pipeline);
  }

  /**
   * Holds a Dataset or values for deferred conversion to a Dataset if needed. PCollections are
   * sometimes created from a collection of objects using
   * {@link SQLContext#createDataset(List, Encoder)}) and then
   * only used to create View objects; in which case they do not need to be
   * converted to bytes since they are not transferred across the network until they are
   * broadcast.
   */
  private class DatasetHolder<T> {

    private Iterable<WindowedValue<T>> windowedValues;
    private Coder<T> coder;
    private Dataset<WindowedValue<T>> dataset;

    DatasetHolder(Iterable<T> values, Coder<T> coder) {
      this.windowedValues =
          Iterables.transform(values, WindowingHelpers.<T>windowValueFunction());
      this.coder = coder;
    }

    DatasetHolder(Dataset<WindowedValue<T>> dataset) {
      this.dataset = dataset;
    }

    @SuppressWarnings("unchecked")
    Dataset<WindowedValue<T>> getDataset() {
      if (dataset == null) {
        // validate windowedValues is not empty because I use it to get the class for the
        // bean Encoder
        //TODO: should we allow to create an empty Dataset/PCollection ?
        if (!windowedValues.iterator().hasNext()) {
          throw new IllegalArgumentException("Cannot create a Dataset for empty values!");
        }
        WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
            WindowedValue.getValueOnlyCoder(coder);
        dataset = sqlContext.createDataset(CoderHelpers.toByteArrays(windowedValues, windowCoder),
            Encoders.BINARY())
            .map(CoderHelpers.fromByteFunctionDatasets(windowCoder),
            Encoders.bean((Class<WindowedValue<T>>) windowedValues.iterator().next().getClass()));
      }
      return dataset;
    }

    Iterable<WindowedValue<T>> getValues(PCollection<T> pcollection) {
      if (windowedValues == null) {
        WindowFn<?, ?> windowFn =
                pcollection.getWindowingStrategy().getWindowFn();
        Coder<? extends BoundedWindow> windowCoder = windowFn.windowCoder();
        final WindowedValue.WindowedValueCoder<T> windowedValueCoder;
            if (windowFn instanceof GlobalWindows) {
              windowedValueCoder =
                  WindowedValue.ValueOnlyWindowedValueCoder.of(pcollection.getCoder());
            } else {
              windowedValueCoder =
                  WindowedValue.FullWindowedValueCoder.of(pcollection.getCoder(), windowCoder);
            }
        Dataset<byte[]> bytesDataset =
            dataset.map(CoderHelpers.toByteFunctionDatasets(windowedValueCoder), Encoders.BINARY());
        List<byte[]> clientBytes = bytesDataset.collectAsList();
        windowedValues = Iterables.transform(clientBytes,
            new Function<byte[], WindowedValue<T>>() {
          @Override
          public WindowedValue<T> apply(byte[] bytes) {
            return CoderHelpers.fromByteArray(bytes, windowedValueCoder);
          }
        });
      }
      return windowedValues;
    }
  }

  protected JavaSparkContext getSparkContext() {
    return jsc;
  }

  public SQLContext getSqlContext() {
    return sqlContext;
  }

  protected Pipeline getPipeline() {
    return pipeline;
  }

  protected SparkRuntimeContext getRuntimeContext() {
    return runtime;
  }

  protected void setCurrentTransform(AppliedPTransform<?, ?, ?> transform) {
    this.currentTransform = transform;
  }

  protected AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  protected <I extends PInput> I getInput(PTransform<I, ?> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    I input = (I) currentTransform.getInput();
    return input;
  }

  protected <O extends POutput> O getOutput(PTransform<?, O> transform) {
    checkArgument(currentTransform != null && currentTransform.getTransform() == transform,
        "can only be called with current transform");
    @SuppressWarnings("unchecked")
    O output = (O) currentTransform.getOutput();
    return output;
  }

  protected  <T> void setOutputDataset(PTransform<?, ?> transform,
      Dataset<WindowedValue<T>> dataset) {
    setDataset((PValue) getOutput(transform), dataset);
  }

  protected  <T> void setOutputDatasetFromValues(PTransform<?, ?> transform, Iterable<T> values,
      Coder<T> coder) {
    pcollections.put((PValue) getOutput(transform), new DatasetHolder<>(values, coder));
  }

  void setPView(PValue view, Iterable<? extends WindowedValue<?>> value) {
    pview.put(view, value);
  }

  protected boolean hasOutputDataset(PTransform<? extends PInput, ?> transform) {
    PValue pvalue = (PValue) getOutput(transform);
    return pcollections.containsKey(pvalue);
  }

  protected Dataset<?> getDataset(PValue pvalue) {
    DatasetHolder<?> datasetHolder = pcollections.get(pvalue);
    Dataset<?> dataset = datasetHolder.getDataset();
    leafDatasets.remove(datasetHolder);
    if (multireads.contains(pvalue)) {
      // Ensure the Dataset is marked as cached
      dataset.cache();
    } else {
      multireads.add(pvalue);
    }
    return dataset;
  }

  protected <T> void setDataset(PValue pvalue, Dataset<WindowedValue<T>> dataset) {
    try {
      //TODO: ??
      dataset.as(pvalue.getName());
    } catch (IllegalStateException e) {
      // name not set, ignore
    }
    DatasetHolder<T> datasetHolder = new DatasetHolder<>(dataset);
    pcollections.put(pvalue, datasetHolder);
    leafDatasets.add(datasetHolder);
  }

  Dataset<?> getInputDataset(PTransform<? extends PInput, ?> transform) {
    return getDataset((PValue) getInput(transform));
  }


  <T> Iterable<? extends WindowedValue<?>> getPCollectionView(PCollectionView<T> view) {
    return pview.get(view);
  }

  /**
   * Computes the outputs for all Datasets that are leaves in the DAG and do not have any
   * actions (like saving to a file) registered on them (i.e. they are performed for side
   * effects).
   */
  public void computeOutputs() {
    for (DatasetHolder<?> datasetHolder : leafDatasets) {
      Dataset<?> dataset = datasetHolder.getDataset();
      dataset.cache(); // cache so that any subsequent get() is cheap
      dataset.count(); // force the Dataset to be computed
    }
  }

  @Override
  public <T> T get(PValue value) {
    if (pobjects.containsKey(value)) {
      @SuppressWarnings("unchecked")
      T result = (T) pobjects.get(value);
      return result;
    }
    if (pcollections.containsKey(value)) {
      Dataset<?> dataset = pcollections.get(value).getDataset();
      @SuppressWarnings("unchecked")
      T res = (T) Iterables.getOnlyElement(dataset.collectAsList());
      pobjects.put(value, res);
      return res;
    }
    throw new IllegalStateException("Cannot resolve un-known PObject: " + value);
  }

  @Override
  public <T> T getAggregatorValue(String named, Class<T> resultType) {
    return runtime.getAggregatorValue(named, resultType);
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    return runtime.getAggregatorValues(aggregator);
  }

  @Override
  public <T> Iterable<T> get(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    DatasetHolder<T> datasetHolder = (DatasetHolder<T>) pcollections.get(pcollection);
    Iterable<WindowedValue<T>> windowedValues = datasetHolder.getValues(pcollection);
    return Iterables.transform(windowedValues, WindowingHelpers.<T>unwindowValueFunction());
  }

  <T> Iterable<WindowedValue<T>> getWindowedValues(PCollection<T> pcollection) {
    @SuppressWarnings("unchecked")
    DatasetHolder<T> datasetHolder = (DatasetHolder<T>) pcollections.get(pcollection);
    return datasetHolder.getValues(pcollection);
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
}
