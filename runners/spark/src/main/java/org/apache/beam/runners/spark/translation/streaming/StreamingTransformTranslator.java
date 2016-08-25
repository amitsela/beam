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

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.io.ConsoleIO;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.io.SerializableConfigurationCoder;
import org.apache.beam.runners.spark.io.SparkSource;
import org.apache.beam.runners.spark.io.hadoop.HadoopIO;
import org.apache.beam.runners.spark.translation.DoFnFunction;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AssignWindowsDoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Maps;
import com.google.api.client.util.Sets;
import com.google.common.reflect.TypeToken;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.dstream.InputDStream;
import org.apache.spark.streaming.scheduler.RateController;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

/**
 * Supports translation between a DataFlow transform, and Spark's operations on DStreams.
 */
public final class StreamingTransformTranslator {

  private StreamingTransformTranslator() {
  }

  private static <T> TransformEvaluator<ConsoleIO.Write.Unbound<T>> print() {
    return new TransformEvaluator<ConsoleIO.Write.Unbound<T>>() {
      @Override
      public void evaluate(ConsoleIO.Write.Unbound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>> dstream =
            (JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>>)
            ((StreamingEvaluationContext) context).getStream(transform);
        dstream.map(WindowingHelpers.<T>unwindowFunction()).print(transform.getNum());
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> create() {
    return new TransformEvaluator<Create.Values<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Create.Values<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        Iterable<T> elems = transform.getElements();
        Coder<T> coder = sec.getOutput(transform).getCoder();
        sec.setDStreamFromQueue(transform, Collections.singletonList(elems), coder);
      }
    };
  }

  private static <T> TransformEvaluator<CreateStream.QueuedValues<T>> createFromQueue() {
    return new TransformEvaluator<CreateStream.QueuedValues<T>>() {
      @Override
      public void evaluate(CreateStream.QueuedValues<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        Iterable<Iterable<T>> values = transform.getQueuedValues();
        Coder<T> coder = sec.getOutput(transform).getCoder();
        sec.setDStreamFromQueue(transform, values, coder);
      }
    };
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        PCollectionList<T> pcs = sec.getInput(transform);
        JavaDStream<WindowedValue<T>> first =
            (JavaDStream<WindowedValue<T>>) sec.getStream(pcs.get(0));
        List<JavaDStream<WindowedValue<T>>> rest = Lists.newArrayListWithCapacity(pcs.size() - 1);
        for (int i = 1; i < pcs.size(); i++) {
          rest.add((JavaDStream<WindowedValue<T>>) sec.getStream(pcs.get(i)));
        }
        JavaDStream<WindowedValue<T>> dstream = sec.getStreamingContext().union(first, rest);
        sec.setStream(transform, dstream);
      }
    };
  }

  private static <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT> rddTransform(
      final SparkPipelineTranslator rddTranslator) {
    return new TransformEvaluator<TransformT>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(TransformT transform, EvaluationContext context) {
        TransformEvaluator<TransformT> rddEvaluator =
            rddTranslator.translate((Class<TransformT>) transform.getClass());

        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        if (sec.hasStream(transform)) {
          JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>> dStream =
              (JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>>)
              sec.getStream(transform);

          sec.setStream(transform, dStream
              .transform(new RDDTransform<>(sec, rddEvaluator, transform)));
        } else {
          // if the transformation requires direct access to RDD (not in stream)
          // this is used for "fake" transformations like with PAssert
          rddEvaluator.evaluate(transform, context);
        }
      }
    };
  }

  /**
   * RDD transform function If the transformation function doesn't have an input, create a fake one
   * as an empty RDD.
   *
   * @param <TransformT> PTransform type
   */
  private static final class RDDTransform<TransformT extends PTransform<?, ?>>
      implements Function<JavaRDD<WindowedValue<Object>>, JavaRDD<WindowedValue<Object>>> {

    private final StreamingEvaluationContext context;
    private final AppliedPTransform<?, ?, ?> appliedPTransform;
    private final TransformEvaluator<TransformT> rddEvaluator;
    private final TransformT transform;


    private RDDTransform(StreamingEvaluationContext context,
                         TransformEvaluator<TransformT> rddEvaluator,
        TransformT transform) {
      this.context = context;
      this.appliedPTransform = context.getCurrentTransform();
      this.rddEvaluator = rddEvaluator;
      this.transform = transform;
    }

    @Override
    @SuppressWarnings("unchecked")
    public JavaRDD<WindowedValue<Object>>
        call(JavaRDD<WindowedValue<Object>> rdd) throws Exception {
      AppliedPTransform<?, ?, ?> existingAPT = context.getCurrentTransform();
      context.setCurrentTransform(appliedPTransform);
      context.setInputRDD(transform, rdd);
      rddEvaluator.evaluate(transform, context);
      if (!context.hasOutputRDD(transform)) {
        // fake RDD as output
        context.setOutputRDD(transform,
            context.getSparkContext().<WindowedValue<Object>>emptyRDD());
      }
      JavaRDD<WindowedValue<Object>> outRDD =
          (JavaRDD<WindowedValue<Object>>) context.getOutputRDD(transform);
      context.setCurrentTransform(existingAPT);
      return outRDD;
    }
  }

  @SuppressWarnings("unchecked")
  private static <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT> foreachRDD(
      final SparkPipelineTranslator rddTranslator) {
    return new TransformEvaluator<TransformT>() {
      @Override
      public void evaluate(TransformT transform, EvaluationContext context) {
        TransformEvaluator<TransformT> rddEvaluator =
            rddTranslator.translate((Class<TransformT>) transform.getClass());

        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        if (sec.hasStream(transform)) {
          JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>> dStream =
              (JavaDStreamLike<WindowedValue<Object>, ?, JavaRDD<WindowedValue<Object>>>)
              sec.getStream(transform);

          dStream.foreachRDD(new RDDOutputOperator<>(sec, rddEvaluator, transform));
        } else {
          rddEvaluator.evaluate(transform, context);
        }
      }
    };
  }

  /**
   * RDD output function.
   *
   * @param <TransformT> PTransform type
   */
  private static final class RDDOutputOperator<TransformT extends PTransform<?, ?>>
      implements VoidFunction<JavaRDD<WindowedValue<Object>>> {

    private final StreamingEvaluationContext context;
    private final AppliedPTransform<?, ?, ?> appliedPTransform;
    private final TransformEvaluator<TransformT> rddEvaluator;
    private final TransformT transform;


    private RDDOutputOperator(StreamingEvaluationContext context,
                              TransformEvaluator<TransformT> rddEvaluator, TransformT transform) {
      this.context = context;
      this.appliedPTransform = context.getCurrentTransform();
      this.rddEvaluator = rddEvaluator;
      this.transform = transform;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void call(JavaRDD<WindowedValue<Object>> rdd) throws Exception {
      AppliedPTransform<?, ?, ?> existingAPT = context.getCurrentTransform();
      context.setCurrentTransform(appliedPTransform);
      context.setInputRDD(transform, rdd);
      rddEvaluator.evaluate(transform, context);
      context.setCurrentTransform(existingAPT);
    }
  }

  private static <T> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        WindowFn<? super T, ?> windowFn = transform.getWindowFn();
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<T>> dStream =
            (JavaDStream<WindowedValue<T>>) sec.getStream(transform);
        if (windowFn instanceof FixedWindows) {
          Duration windowDuration = Durations.milliseconds(((FixedWindows) windowFn).getSize()
              .getMillis());
          sec.setStream(transform, dStream.window(windowDuration));
        } else if (windowFn instanceof SlidingWindows) {
          Duration windowDuration = Durations.milliseconds(((SlidingWindows) windowFn).getSize()
              .getMillis());
          Duration slideDuration = Durations.milliseconds(((SlidingWindows) windowFn).getPeriod()
              .getMillis());
          sec.setStream(transform, dStream.window(windowDuration, slideDuration));
        }
        //--- then we apply windowing to the elements
        OldDoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
        DoFnFunction<T, T> dofn = new DoFnFunction<>(addWindowsDoFn,
            ((StreamingEvaluationContext) context).getRuntimeContext(), null);
        @SuppressWarnings("unchecked")
        JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>> dstream =
            (JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>>)
            sec.getStream(transform);
        sec.setStream(transform, dstream.mapPartitions(dofn));
      }
    };
  }

  private static <T> TransformEvaluator<Read.Unbounded<T>> readUnbounded() {
    return new TransformEvaluator<Read.Unbounded<T>>() {
      @Override
      public void evaluate(Read.Unbounded<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        Configuration conf = sec.getStreamingContext().sparkContext().hadoopConfiguration();
        BroadcastHelper<SerializableConfiguration> broadcastedConf =
            BroadcastHelper.create(new SerializableConfiguration(conf),
                SerializableConfigurationCoder.of());
        broadcastedConf.broadcast(sec.getSparkContext());
        //TODO: clean this up
        String checkpointDirectory = "file:/tmp/" + sec.getRuntimeContext()
            .getPipelineOptions().as(SparkPipelineOptions.class).getAppName();

        Path path = new Path(checkpointDirectory);
        try {
          FileSystem fs = path.getFileSystem(conf);
          if (!fs.exists(path)) {
            System.out.println("Creating checkpoint dir at: " + path);
            fs.mkdirs(path);
          } else {
            //TODO: lookup the one-before-last checkpoint dir.
            System.out.println("Starting from checkpoint dir " + path);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }

        boolean bp = RateController.isBackPressureEnabled(sec.getSparkContext().getConf());
        InputDStream<WindowedValue<T>> inputDStream = new SparkSource.SourceDStream<>(
            sec.getStreamingContext().ssc(), transform.getSource(), sec.getRuntimeContext(),
                checkpointDirectory, broadcastedConf, bp);
        JavaDStream<WindowedValue<T>> dStream = new JavaDStream<>(inputDStream,
            scala.reflect.ClassTag$.MODULE$.<WindowedValue<T>>apply(WindowedValue.class));
        sec.setStream(transform, dStream);
      }
    };
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(ConsoleIO.Write.Unbound.class, print());
    EVALUATORS.put(CreateStream.QueuedValues.class, createFromQueue());
    EVALUATORS.put(Create.Values.class, create());
    EVALUATORS.put(Read.Unbounded.class, readUnbounded());
    EVALUATORS.put(Window.Bound.class, window());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
  }

  private static final Set<Class<? extends PTransform>> UNSUPPORTED_EVALUATORS = Sets
      .newHashSet();

  static {
    //TODO - add support for the following
    UNSUPPORTED_EVALUATORS.add(TextIO.Read.Bound.class);
    UNSUPPORTED_EVALUATORS.add(TextIO.Write.Bound.class);
    UNSUPPORTED_EVALUATORS.add(AvroIO.Read.Bound.class);
    UNSUPPORTED_EVALUATORS.add(AvroIO.Write.Bound.class);
    UNSUPPORTED_EVALUATORS.add(HadoopIO.Read.Bound.class);
    UNSUPPORTED_EVALUATORS.add(HadoopIO.Write.Bound.class);
  }

  @SuppressWarnings("unchecked")
  private static <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
      getTransformEvaluator(Class<TransformT> clazz, SparkPipelineTranslator rddTranslator) {
    TransformEvaluator<TransformT> transform =
        (TransformEvaluator<TransformT>) EVALUATORS.get(clazz);
    if (transform == null) {
      if (UNSUPPORTED_EVALUATORS.contains(clazz)) {
        throw new UnsupportedOperationException("Beam transformation " + clazz
          .getCanonicalName()
          + " is currently unsupported by the Spark streaming pipeline");
      }
      // DStream transformations will transform an RDD into another RDD
      // Actions will create output
      // In Beam it depends on the PTransform's Input and Output class
      Class<?> pTOutputClazz = getPTransformOutputClazz(clazz);
      if (PDone.class.equals(pTOutputClazz)) {
        return foreachRDD(rddTranslator);
      } else {
        return rddTransform(rddTranslator);
      }
    }
    return transform;
  }

  private static <TransformT extends PTransform<?, ?>> Class<?>
  getPTransformOutputClazz(Class<TransformT> clazz) {
    Type[] types = ((ParameterizedType) clazz.getGenericSuperclass()).getActualTypeArguments();
    return TypeToken.of(clazz).resolveType(types[1]).getRawType();
  }

  /**
   * Translator matches Dataflow transformation with the appropriate Spark streaming evaluator.
   * rddTranslator uses Spark evaluators in transform/foreachRDD to evaluate the transformation
   */
  public static class Translator implements SparkPipelineTranslator {

    private final SparkPipelineTranslator rddTranslator;

    public Translator(SparkPipelineTranslator rddTranslator) {
      this.rddTranslator = rddTranslator;
    }

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      // streaming includes rdd transformations as well
      return EVALUATORS.containsKey(clazz) || rddTranslator.hasTranslation(clazz);
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
    translate(Class<TransformT> clazz) {
      return getTransformEvaluator(clazz, rddTranslator);
    }
  }
}
