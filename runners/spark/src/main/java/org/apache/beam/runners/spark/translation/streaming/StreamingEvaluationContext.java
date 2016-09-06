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
package org.apache.beam.runners.spark.translation.streaming;


import com.google.common.collect.Iterables;

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Streaming evaluation context helps to handle streaming.
 */
public class StreamingEvaluationContext extends EvaluationContext {

  private final JavaStreamingContext jssc;
  private final long timeout;

  public StreamingEvaluationContext(JavaSparkContext jsc, Pipeline pipeline,
      JavaStreamingContext jssc, long timeout) {
    super(jsc, pipeline);
    this.jssc = jssc;
    this.timeout = timeout;
  }


  <T> void setDStreamFromQueue(
      PTransform<?, ?> transform, Iterable<Iterable<T>> values, Coder<T> coder) {
    WindowedValue.ValueOnlyWindowedValueCoder<T> windowCoder =
          WindowedValue.getValueOnlyCoder(coder);
      // create the DStream from queue
      Queue<JavaRDD<WindowedValue<T>>> rddQueue = new LinkedBlockingQueue<>();
      for (Iterable<T> v : values) {
        Iterable<WindowedValue<T>> windowedValues =
            Iterables.transform(v, WindowingHelpers.<T>windowValueFunction());
        JavaRDD<WindowedValue<T>> rdd = jssc.sc().parallelize(
            CoderHelpers.toByteArrays(windowedValues, windowCoder)).map(
                CoderHelpers.fromByteFunction(windowCoder));
        rddQueue.offer(rdd);
      }
      // create dstream from queue, one at a time, no defaults
      // mainly for unit test so no reason to have this configurable
      JavaDStream<WindowedValue<T>> dStream = jssc.queueStream(rddQueue, true);

    datasets.put((PValue) getOutput(transform), new UnboundedDataset<>(dStream));
  }

  public JavaStreamingContext getStreamingContext() {
    return jssc;
  }

  @Override
  public void close() {
    if (timeout > 0) {
      jssc.awaitTerminationOrTimeout(timeout);
    } else {
      jssc.awaitTermination();
    }
    //TODO: stop gracefully ?
    jssc.stop(false, false);
    state = State.DONE;
    super.close();
  }

  private State state = State.RUNNING;

  @Override
  public State getState() {
    return state;
  }
}
