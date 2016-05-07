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

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.coders.EncoderHelpers;
import org.apache.beam.runners.spark.io.hadoop.ShardNameTemplateHelper;
import org.apache.beam.runners.spark.io.hadoop.TemplatedTextOutputFormat;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AssignWindowsDoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.GroupedDataset;
import org.apache.spark.sql.expressions.Aggregator;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputDirectory;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFilePrefix;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.getOutputFileTemplate;
import static org.apache.beam.runners.spark.io.hadoop.ShardNameBuilder.replaceShardCount;

/**
 * Supports translation between a Beam transform, and Spark's operations on Datasets.
 */
public final class DatasetsTransformTranslator {

  private DatasetsTransformTranslator() {
  }

  public static class FieldGetter {
    private final Map<String, Field> fields;

    public FieldGetter(Class<?> clazz) {
      this.fields = Maps.newHashMap();
      for (Field f : clazz.getDeclaredFields()) {
        f.setAccessible(true);
        this.fields.put(f.getName(), f);
      }
    }

    public <T> T get(String fieldname, Object value) {
      try {
        @SuppressWarnings("unchecked")
        T fieldValue = (T) fields.get(fieldname).get(value);
        return fieldValue;
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform,
          EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        PCollectionList<T> pcs = context.getInput(transform);
        Dataset<WindowedValue<T>> flattened =
            context.getSqlContext().emptyDataFrame().as(EncoderHelpers.<WindowedValue<T>>encode());
        for (int i = 0; i < pcs.size(); i++) {
          flattened = flattened.union((Dataset<WindowedValue<T>>) context.getDataset(pcs.get(i)));
        }
        context.setOutputDataset(transform, flattened);
      }
    };
  }

  private static <K, V> TransformEvaluator<GroupByKey<K, V>> gbk() {
    return new TransformEvaluator<GroupByKey<K, V>>() {
      @Override
      public void evaluate(GroupByKey<K, V> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        @SuppressWarnings("unchecked")
        Dataset<WindowedValue<KV<K, V>>> inDataset =
            (Dataset<WindowedValue<KV<K, V>>>) context.getInputDataset(transform);


//        @SuppressWarnings("unchecked")
//        KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
//        Coder<K> keyCoder = coder.getKeyCoder();
//        final Coder<V> valueCoder = coder.getValueCoder();

        //TODO: transform inDataset into Dataset<Tuple2<K as byte[],
        // WindowedValue<KV<K, V>> as byte[]>> which will be trivial for grouping
        // use coders for that


        GroupedDataset<WindowedValue<K>, WindowedValue<KV<K, V>>> groupedDataset = inDataset
            .groupBy(new MapFunction<WindowedValue<KV<K, V>>, WindowedValue<K>>() {
              @Override
              public WindowedValue<K> call(WindowedValue<KV<K, V>> wkv) throws Exception {
                return WindowedValue.of(wkv.getValue().getKey(), wkv.getTimestamp(),
                        wkv.getWindows(), wkv.getPane());
              }
            }, EncoderHelpers.<WindowedValue<K>>encode());

        context.setOutputGroupedDataset(transform, groupedDataset);
      }
    };
  }

  private static final FieldGetter GROUPED_FG = new FieldGetter(Combine.GroupedValues.class);

  private static <K, VI, VA, VO> TransformEvaluator<Combine.GroupedValues<K, VI, VO>> grouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, VI, VO>>() {
      @Override
      public void evaluate(Combine.GroupedValues<K, VI, VO> transform,
          EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        final Combine.KeyedCombineFn<K, VI, VA, VO> keyed = GROUPED_FG.get("fn", transform);

        Aggregator<WindowedValue<KV<K, VI>>, VA, VO> agg =
            new Aggregator<WindowedValue<KV<K, VI>>, VA, VO>() {
              @Override
              public VA zero() {
                return keyed.createAccumulator(null);
              }

              @Override
              public VA reduce(VA vb, WindowedValue<KV<K, VI>> a) {
                return keyed.addInput(null, vb, a.getValue().getValue());
              }

              @Override
              public VA merge(VA b1, VA b2) {
                return keyed.mergeAccumulators(null, ImmutableList.of(b1, b2));
              }

              @Override
              public VO finish(VA reduction) {
                return keyed.extractOutput(null, reduction);
              }
            };

        @SuppressWarnings("unchecked")
        GroupedDataset<WindowedValue<K>, WindowedValue<KV<K, VI>>> groupedDataset =
            (GroupedDataset<WindowedValue<K>, WindowedValue<KV<K, VI>>>) context.getInputGroupedDataset(transform);
        Dataset<WindowedValue<KV<K, VO>>> outDataset =
            groupedDataset.agg(agg.toColumn(EncoderHelpers.<VA>encode(),
                    EncoderHelpers.<VO>encode()))
            .map(new MapFunction<Tuple2<WindowedValue<K>, VO>, WindowedValue<KV<K, VO>>>() {
              @Override
              public WindowedValue<KV<K, VO>> call(Tuple2<WindowedValue<K>, VO> tuple2) throws
                      Exception {
                WindowedValue<K> wk = tuple2._1();
                return WindowedValue.of(KV.of(wk.getValue(), tuple2._2()), wk.getTimestamp(),
                    wk.getWindows(), wk.getPane());
              }
            }, EncoderHelpers.<WindowedValue<KV<K, VO>>>encode());
        context.setOutputDataset(transform, outDataset);
      }
    };
  }

//  private static final FieldGetter COMBINE_GLOBALLY_FG = new FieldGetter(Combine.Globally.class);

//  private static <I, A, O> TransformEvaluator<Combine.Globally<I, O>> combineGlobally() {
//    return new TransformEvaluator<Combine.Globally<I, O>>() {
//
//      @Override
//      public void evaluate(Combine.Globally<I, O> transform, EvaluationResult context) {
//        final Combine.CombineFn<I, A, O> globally = COMBINE_GLOBALLY_FG.get("fn", transform);
//
//        @SuppressWarnings("unchecked")
//        JavaRDDLike<WindowedValue<I>, ?> inRdd =
//            (JavaRDDLike<WindowedValue<I>, ?>) context.getInputDataset(transform);
//
//        final Coder<I> iCoder = context.getInput(transform).getCoder();
//        final Coder<A> aCoder;
//        try {
//          aCoder = globally.getAccumulatorCoder(
//              context.getPipeline().getCoderRegistry(), iCoder);
//        } catch (CannotProvideCoderException e) {
//          throw new IllegalStateException("Could not determine coder for accumulator", e);
//        }
//
//        // Use coders to convert objects in the PCollection to byte arrays, so they
//        // can be transferred over the network for the shuffle.
//        JavaRDD<byte[]> inRddBytes = inRdd
//            .map(WindowingHelpers.<I>unwindowFunction())
//            .map(CoderHelpers.toByteFunction(iCoder));
//
//        /*A*/ byte[] acc = inRddBytes.aggregate(
//            CoderHelpers.toByteArray(globally.createAccumulator(), aCoder),
//            new Function2</*A*/ byte[], /*I*/ byte[], /*A*/ byte[]>() {
//              @Override
//              public /*A*/ byte[] call(/*A*/ byte[] ab, /*I*/ byte[] ib) throws Exception {
//                A a = CoderHelpers.fromByteArray(ab, aCoder);
//                I i = CoderHelpers.fromByteArray(ib, iCoder);
//                return CoderHelpers.toByteArray(globally.addInput(a, i), aCoder);
//              }
//            },
//            new Function2</*A*/ byte[], /*A*/ byte[], /*A*/ byte[]>() {
//              @Override
//              public /*A*/ byte[] call(/*A*/ byte[] a1b, /*A*/ byte[] a2b) throws Exception {
//                A a1 = CoderHelpers.fromByteArray(a1b, aCoder);
//                A a2 = CoderHelpers.fromByteArray(a2b, aCoder);
//                // don't use Guava's ImmutableList.of as values may be null
//                List<A> accumulators = Collections.unmodifiableList(Arrays.asList(a1, a2));
//                A merged = globally.mergeAccumulators(accumulators);
//                return CoderHelpers.toByteArray(merged, aCoder);
//              }
//            }
//        );
//        O output = globally.extractOutput(CoderHelpers.fromByteArray(acc, aCoder));
//
//        Coder<O> coder = context.getOutput(transform).getCoder();
//        JavaRDD<byte[]> outRdd = context.getSparkContext().parallelize(
//            // don't use Guava's ImmutableList.of as output may be null
//            CoderHelpers.toByteArrays(Collections.singleton(output), coder));
//        context.setOutputDataset(transform, outRdd.map(CoderHelpers.fromByteFunction(coder))
//            .map(WindowingHelpers.<O>windowFunction()));
//      }
//    };
//  }

  private static final FieldGetter COMBINE_PERKEY_FG = new FieldGetter(Combine.PerKey.class);

  private static <K, VI, VA, VO> TransformEvaluator<Combine.PerKey<K, VI, VO>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, VI, VO>>() {
      @Override
      public void evaluate(Combine.PerKey<K, VI, VO> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        final Combine.KeyedCombineFn<K, VI, VA, VO> keyed = COMBINE_PERKEY_FG.get("fn", transform);
        Aggregator<WindowedValue<KV<K, VI>>, WindowedValue<KV<K, VA>>, WindowedValue<VO>> agg =
            new Aggregator<WindowedValue<KV<K, VI>>,
            WindowedValue<KV<K, VA>>, WindowedValue<VO>>() {
          @Override
          public WindowedValue<KV<K, VA>> zero() {
            return null;
          }

          @Override
          public WindowedValue<KV<K, VA>> reduce(WindowedValue<KV<K, VA>> wkva,
                                                 WindowedValue<KV<K, VI>> wkvi) {
            K key = wkvi.getValue().getKey();
            if (wkva == null) {
              // create accumulator if not created yet
              VA va = keyed.createAccumulator(key);
              wkva = composeWkv(va, wkvi);
            }
            // add input to accumulator
            VA va = keyed.addInput(key, wkva.getValue().getValue(), wkvi.getValue().getValue());
            return composeWkv(va, wkvi);
          }

          private <VV, VW> WindowedValue<KV<K, VV>> composeWkv(VV va, WindowedValue<KV<K, VW>> wkvv) {
            return WindowedValue.of(KV.of(wkvv.getValue().getKey(), va), wkvv.getTimestamp(),
                wkvv.getWindows(), wkvv.getPane());
          }

          @Override
          public WindowedValue<KV<K, VA>> merge(WindowedValue<KV<K, VA>> wkva1,
                                                WindowedValue<KV<K, VA>> wkva2) {
            if (wkva1 == null) {
              return wkva2;
            } else if (wkva2 == null) {
              return wkva1;
            } else {
              VA va = keyed.mergeAccumulators(wkva1.getValue().getKey(),
                  Collections.unmodifiableList(Arrays.asList(wkva1.getValue().getValue(),
                  wkva2.getValue().getValue())));
              return composeWkv(va, wkva1);
            }
          }

          @Override
          public WindowedValue<VO> finish(WindowedValue<KV<K, VA>> wkva) {
            K key = wkva.getValue().getKey();
            VA va = wkva.getValue().getValue();
            VO vo = keyed.extractOutput(key, va);
            return WindowedValue.of(vo, wkva.getTimestamp(), wkva.getWindows(), wkva.getPane());
          }
        };

        @SuppressWarnings("unchecked")
        Dataset<WindowedValue<KV<K, VI>>> inDataset =
            (Dataset<WindowedValue<KV<K, VI>>>) context.getInputDataset(transform);
        inDataset.flatMap(new FlatMapFunction<WindowedValue<KV<K,VI>>, WindowedValue<KV<K,VI>>>() {
          @Override
          public Iterable<WindowedValue<KV<K, VI>>>
          call(WindowedValue<KV<K, VI>> wkv) throws Exception {
            List<WindowedValue<KV<K, VI>>> windowedValues = Lists.newArrayList();
            // flatMap all windows this value belongs to
            for (BoundedWindow boundedWindow: wkv.getWindows()) {
              KV<K, VI> kv = wkv.getValue();
              windowedValues.add(WindowedValue.of(kv, boundedWindow.maxTimestamp(), boundedWindow,
                  wkv.getPane()));
            }
            return Iterables.unmodifiableIterable(windowedValues);
          }
        }, EncoderHelpers.<WindowedValue<KV<K, VI>>>encode());


//        @SuppressWarnings("unchecked")
//        KvCoder<K, VI> inputCoder = (KvCoder<K, VI>) context.getInput(transform).getCoder();
//        Coder<K> keyCoder = inputCoder.getKeyCoder();
//        Coder<VI> viCoder = inputCoder.getValueCoder();
//        Coder<VA> vaCoder;
//        try {
//          vaCoder = keyed.getAccumulatorCoder(
//              context.getPipeline().getCoderRegistry(), keyCoder, viCoder);
//        } catch (CannotProvideCoderException e) {
//          throw new IllegalStateException("Could not determine coder for accumulator", e);
//        }
//        Coder<KV<K, VI>> kviCoder = KvCoder.of(keyCoder, viCoder);
//        Coder<KV<K, VA>> kvaCoder = KvCoder.of(keyCoder, vaCoder);
        //-- windowed coders
//        final WindowedValue.FullWindowedValueCoder<K> wkCoder =
//                WindowedValue.FullWindowedValueCoder.of(keyCoder,
//                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());
//        final WindowedValue.FullWindowedValueCoder<KV<K, VI>> wkviCoder =
//                WindowedValue.FullWindowedValueCoder.of(kviCoder,
//                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());
//        final WindowedValue.FullWindowedValueCoder<KV<K, VA>> wkvaCoder =
//                WindowedValue.FullWindowedValueCoder.of(kvaCoder,
//                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());

        //TODO: Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.


        GroupedDataset<WindowedValue<K>, WindowedValue<KV<K, VI>>> grouped =
            inDataset.groupBy(new MapFunction<WindowedValue<KV<K, VI>>, WindowedValue<K>>() {
              @Override
              public WindowedValue<K> call(WindowedValue<KV<K, VI>> wkv) throws Exception {
                K key = wkv.getValue().getKey();
                return WindowedValue.of(key, wkv.getTimestamp(), wkv.getWindows(), wkv.getPane());
              }
            }, EncoderHelpers.<WindowedValue<K>>encode());
        Dataset<Tuple2<WindowedValue<K>, WindowedValue<VO>>> aggregated =
            grouped.agg(agg.toColumn(EncoderHelpers.<WindowedValue<KV<K, VA>>>encode(),
            EncoderHelpers.<WindowedValue<VO>>encode()));
        context.setOutputDataset(transform, aggregated.map(
                new MapFunction<Tuple2<WindowedValue<K>, WindowedValue<VO>>,
                        WindowedValue<KV<K, VO>>>() {

                  @Override
                  public WindowedValue<KV<K, VO>> call(Tuple2<WindowedValue<K>,
                          WindowedValue<VO>> tuple2) throws Exception {
                    K key = tuple2._1().getValue();
                    WindowedValue<VO> wvo = tuple2._2();
                    return WindowedValue.of(KV.of(key, wvo.getValue()), wvo.getTimestamp(),
                        wvo.getWindows(), wvo.getPane());
                  }
                }, EncoderHelpers.<WindowedValue<KV<K, VO>>>encode()));

      }
    };
  }

  private static final class KVFunction<K, VI, VO>
      implements Function<WindowedValue<KV<K, Iterable<VI>>>, WindowedValue<KV<K, VO>>>, 
      MapFunction<WindowedValue<KV<K, Iterable<VI>>>, WindowedValue<KV<K, VO>>>{
    private final Combine.KeyedCombineFn<K, VI, ?, VO> keyed;

     KVFunction(Combine.KeyedCombineFn<K, VI, ?, VO> keyed) {
      this.keyed = keyed;
    }

    @Override
    public WindowedValue<KV<K, VO>> call(WindowedValue<KV<K, Iterable<VI>>> windowedKv)
        throws Exception {
      KV<K, Iterable<VI>> kv = windowedKv.getValue();
      return WindowedValue.of(KV.of(kv.getKey(), keyed.apply(kv.getKey(), kv.getValue())),
          windowedKv.getTimestamp(), windowedKv.getWindows(), windowedKv.getPane());
    }
  }

  private static <K, V> JavaPairRDD<K, V> toPair(JavaRDDLike<KV<K, V>, ?> rdd) {
    return rdd.mapToPair(new PairFunction<KV<K, V>, K, V>() {
      @Override
      public Tuple2<K, V> call(KV<K, V> kv) {
        return new Tuple2<>(kv.getKey(), kv.getValue());
      }
    });
  }

  private static <K, V> JavaRDDLike<KV<K, V>, ?> fromPair(JavaPairRDD<K, V> rdd) {
    return rdd.map(new Function<Tuple2<K, V>, KV<K, V>>() {
      @Override
      public KV<K, V> call(Tuple2<K, V> t2) {
        return KV.of(t2._1(), t2._2());
      }
    });
  }

  private static <I, O> TransformEvaluator<ParDo.Bound<I, O>> parDo() {
    return new TransformEvaluator<ParDo.Bound<I, O>>() {
      @Override
      public void evaluate(ParDo.Bound<I, O> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        DoFnFunction<I, O> dofn =
            new DoFnFunction<>(transform.getFn(),
                context.getRuntimeContext(),
                getSideInputs(transform.getSideInputs(), context));
        @SuppressWarnings("unchecked")
        Dataset<WindowedValue<I>> dataset =
            (Dataset<WindowedValue<I>>) context.getInputDataset(transform);
        context.setOutputDataset(transform, dataset.mapPartitions(dofn,
                EncoderHelpers.<WindowedValue<O>>encode()));
      }
    };
  }

//  private static final FieldGetter MULTIDO_FG = new FieldGetter(ParDo.BoundMulti.class);
//
//  private static <I, O> TransformEvaluator<ParDo.BoundMulti<I, O>> multiDo() {
//    return new TransformEvaluator<ParDo.BoundMulti<I, O>>() {
//      @Override
//      public void evaluate(ParDo.BoundMulti<I, O> transform, EvaluationResult ctxt) {
//        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
//        TupleTag<O> mainOutputTag = MULTIDO_FG.get("mainOutputTag", transform);
//        MultiDoFnFunction<I, O> multifn = new MultiDoFnFunction<>(
//            transform.getFn(),
//            context.getRuntimeContext(),
//            mainOutputTag,
//            getSideInputs(transform.getSideInputs(), context));
//
//        @SuppressWarnings("unchecked")
//        Dataset<WindowedValue<I>> inDataset =
//            (Dataset<WindowedValue<I>>) context.getInputDataset(transform);
//        Dataset<TupleTag<?>, WindowedValue<?>> all = inDataset.mapPartitions(multifn).cache();
//
//        PCollectionTuple pct = context.getOutput(transform);
//        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
//          @SuppressWarnings("unchecked")
//          JavaPairRDD<TupleTag<?>, WindowedValue<?>> filtered =
//              all.filter(new TupleTagFilter(e.getKey()));
//          @SuppressWarnings("unchecked")
//          // Object is the best we can do since different outputs can have different tags
//                  JavaRDD<WindowedValue<Object>> values =
//              (JavaRDD<WindowedValue<Object>>) (JavaRDD<?>) filtered.values();
//          context.setDataset(e.getValue(), values);
//        }
//      }
//    };
//  }


  private static <T> TransformEvaluator<TextIO.Read.Bound<T>> readText() {
    return new TransformEvaluator<TextIO.Read.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Read.Bound<T> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        String pattern = transform.getFilepattern();
        Dataset<WindowedValue<String>> dataset =
            context.getSqlContext().read().text(pattern).as(Encoders.STRING())
            .map(WindowingHelpers.<String>windowFunctionDatasets(),
            EncoderHelpers.<WindowedValue<String>>encode());
        context.setOutputDataset(transform, dataset);
      }
    };
  }

  private static <T> TransformEvaluator<TextIO.Write.Bound<T>> writeText() {
    return new TransformEvaluator<TextIO.Write.Bound<T>>() {
      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(TextIO.Write.Bound<T> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        Dataset<Tuple2<T, Void>> last =
            ((Dataset<WindowedValue<T>>) context.getInputDataset(transform))
            .map(WindowingHelpers.<T>unwindowFunctionDatasets(),
            EncoderHelpers.<T>encode())
            .map(new MapFunction<T, Tuple2<T,Void>>() {
              @Override
              public Tuple2<T, Void> call(T t) throws Exception {
                return new Tuple2<>(t, null);
              }
            }, Encoders.tuple(EncoderHelpers.<T>encode(), Encoders.bean(Void.class)));
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
                transform.getShardTemplate(), transform.getFilenamePrefix(),
                transform.getFilenameSuffix());
        writeHadoopFile(last, new Configuration(), shardTemplateInfo, Text.class,
            NullWritable.class, TemplatedTextOutputFormat.class);
      }
    };
  }

//  private static <T> TransformEvaluator<AvroIO.Read.Bound<T>> readAvro() {
//    return new TransformEvaluator<AvroIO.Read.Bound<T>>() {
//      @Override
//      public void evaluate(AvroIO.Read.Bound<T> transform, EvaluationResult context) {
//        String pattern = transform.getFilepattern();
//        JavaSparkContext jsc = context.getSparkContext();
//        @SuppressWarnings("unchecked")
//        JavaRDD<AvroKey<T>> avroFile = (JavaRDD<AvroKey<T>>) (JavaRDD<?>)
//            jsc.newAPIHadoopFile(pattern,
//                                 AvroKeyInputFormat.class,
//                                 AvroKey.class, NullWritable.class,
//                                 new Configuration()).keys();
//        JavaRDD<WindowedValue<T>> rdd = avroFile.map(
//            new Function<AvroKey<T>, T>() {
//              @Override
//              public T call(AvroKey<T> key) {
//                return key.datum();
//              }
//            }).map(WindowingHelpers.<T>windowFunction());
//        context.setOutputDataset(transform, rdd);
//      }
//    };
//  }

//  private static <T> TransformEvaluator<AvroIO.Write.Bound<T>> writeAvro() {
//    return new TransformEvaluator<AvroIO.Write.Bound<T>>() {
//      @Override
//      public void evaluate(AvroIO.Write.Bound<T> transform, EvaluationResult context) {
//        Job job;
//        try {
//          job = Job.getInstance();
//        } catch (IOException e) {
//          throw new IllegalStateException(e);
//        }
//        AvroJob.setOutputKeySchema(job, transform.getSchema());
//        @SuppressWarnings("unchecked")
//        JavaPairRDD<AvroKey<T>, NullWritable> last =
//            ((JavaRDDLike<WindowedValue<T>, ?>) context.getInputDataset(transform))
//            .map(WindowingHelpers.<T>unwindowFunction())
//            .mapToPair(new PairFunction<T, AvroKey<T>, NullWritable>() {
//              @Override
//              public Tuple2<AvroKey<T>, NullWritable> call(T t) throws Exception {
//                return new Tuple2<>(new AvroKey<>(t), NullWritable.get());
//              }
//            });
//        ShardTemplateInformation shardTemplateInfo =
//            new ShardTemplateInformation(transform.getNumShards(),
//            transform.getShardTemplate(), transform.getFilenamePrefix(),
//            transform.getFilenameSuffix());
//        writeHadoopFile(last, job.getConfiguration(), shardTemplateInfo,
//            AvroKey.class, NullWritable.class, TemplatedAvroKeyOutputFormat.class);
//      }
//    };
//  }

//  private static <K, V> TransformEvaluator<HadoopIO.Read.Bound<K, V>> readHadoop() {
//    return new TransformEvaluator<HadoopIO.Read.Bound<K, V>>() {
//      @Override
//      public void evaluate(HadoopIO.Read.Bound<K, V> transform, EvaluationResult context) {
//        String pattern = transform.getFilepattern();
//        JavaSparkContext jsc = context.getSparkContext();
//        @SuppressWarnings ("unchecked")
//        JavaPairRDD<K, V> file = jsc.newAPIHadoopFile(pattern,
//            transform.getFormatClass(),
//            transform.getKeyClass(), transform.getValueClass(),
//            new Configuration());
//        JavaRDD<WindowedValue<KV<K, V>>> rdd =
//            file.map(new Function<Tuple2<K, V>, KV<K, V>>() {
//          @Override
//          public KV<K, V> call(Tuple2<K, V> t2) throws Exception {
//            return KV.of(t2._1(), t2._2());
//          }
//        }).map(WindowingHelpers.<KV<K, V>>windowFunction());
//        context.setOutputDataset(transform, rdd);
//      }
//    };
//  }

//  private static <K, V> TransformEvaluator<HadoopIO.Write.Bound<K, V>> writeHadoop() {
//    return new TransformEvaluator<HadoopIO.Write.Bound<K, V>>() {
//      @Override
//      public void evaluate(HadoopIO.Write.Bound<K, V> transform, EvaluationResult context) {
//        @SuppressWarnings("unchecked")
//        JavaPairRDD<K, V> last = ((JavaRDDLike<WindowedValue<KV<K, V>>, ?>) context
//            .getInputDataset(transform))
//            .map(WindowingHelpers.<KV<K, V>>unwindowFunction())
//            .mapToPair(new PairFunction<KV<K, V>, K, V>() {
//              @Override
//              public Tuple2<K, V> call(KV<K, V> t) throws Exception {
//                return new Tuple2<>(t.getKey(), t.getValue());
//              }
//            });
//        ShardTemplateInformation shardTemplateInfo =
//            new ShardTemplateInformation(transform.getNumShards(),
//                transform.getShardTemplate(), transform.getFilenamePrefix(),
//                transform.getFilenameSuffix());
//        Configuration conf = new Configuration();
//        for (Map.Entry<String, String> e : transform.getConfigurationProperties().entrySet()) {
//          conf.set(e.getKey(), e.getValue());
//        }
//        writeHadoopFile(last, conf, shardTemplateInfo,
//            transform.getKeyClass(), transform.getValueClass(), transform.getFormatClass());
//      }
//    };
//  }

  private static final class ShardTemplateInformation {
    private final int numShards;
    private final String shardTemplate;
    private final String filenamePrefix;
    private final String filenameSuffix;

    private ShardTemplateInformation(int numShards, String shardTemplate, String
        filenamePrefix, String filenameSuffix) {
      this.numShards = numShards;
      this.shardTemplate = shardTemplate;
      this.filenamePrefix = filenamePrefix;
      this.filenameSuffix = filenameSuffix;
    }

    int getNumShards() {
      return numShards;
    }

    String getShardTemplate() {
      return shardTemplate;
    }

    String getFilenamePrefix() {
      return filenamePrefix;
    }

    String getFilenameSuffix() {
      return filenameSuffix;
    }
  }

  private static <K, V> void writeHadoopFile(Dataset<Tuple2<K, V>> dataset, Configuration conf,
      ShardTemplateInformation shardTemplateInfo, Class<?> keyClass, Class<?> valueClass,
      Class<? extends FileOutputFormat> formatClass) {
    int numShards = shardTemplateInfo.getNumShards();
    String shardTemplate = shardTemplateInfo.getShardTemplate();
    String filenamePrefix = shardTemplateInfo.getFilenamePrefix();
    String filenameSuffix = shardTemplateInfo.getFilenameSuffix();
    if (numShards != 0) {
      // number of shards was set explicitly, so repartition
      dataset = dataset.repartition(numShards);
    }
    //FIXME: is there a way to get number of partitions from Dataset
//    int actualNumShards = dataset.partitions().size();
//    String template = replaceShardCount(shardTemplate, actualNumShards);
    String template = replaceShardCount(shardTemplate, numShards);
    String outputDir = getOutputDirectory(filenamePrefix, template);
    String filePrefix = getOutputFilePrefix(filenamePrefix, template);
    String fileTemplate = getOutputFileTemplate(filenamePrefix, template);

    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_PREFIX, filePrefix);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_TEMPLATE, fileTemplate);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_SUFFIX, filenameSuffix);
    dataset.rdd().toJavaRDD().mapToPair(new PairFunction<Tuple2<K,V>, K, V>() {
      @Override
      public Tuple2<K, V> call(Tuple2<K, V> kvTuple2) throws Exception {
        return kvTuple2;
      }
    }).saveAsNewAPIHadoopFile(outputDir, keyClass, valueClass, formatClass, conf);
  }

  private static final FieldGetter WINDOW_FG = new FieldGetter(Window.Bound.class);

  private static <T, W extends BoundedWindow> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        @SuppressWarnings("unchecked")
        Dataset<WindowedValue<T>> dataset =
            (Dataset<WindowedValue<T>>) context.getInputDataset(transform);
        WindowFn<? super T, W> windowFn = WINDOW_FG.get("windowFn", transform);
        if (windowFn instanceof GlobalWindows) {
          context.setOutputDataset(transform, dataset);
        } else {
          @SuppressWarnings("unchecked")
          DoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          DoFnFunction<T, T> dofn =
              new DoFnFunction<>(addWindowsDoFn, context.getRuntimeContext(), null);
          context.setOutputDataset(transform, dataset.mapPartitions(dofn,
              EncoderHelpers.<WindowedValue<T>>encode()));
        }
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> create() {
    return new TransformEvaluator<Create.Values<T>>() {
      @Override
      public void evaluate(Create.Values<T> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        Iterable<T> elems = transform.getElements();
        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = context.getOutput(transform).getCoder();
        context.setOutputDatasetFromValues(transform, elems, coder);
      }
    };
  }

  private static <T> TransformEvaluator<View.AsSingleton<T>> viewAsSingleton() {
    return new TransformEvaluator<View.AsSingleton<T>>() {
      @Override
      public void evaluate(View.AsSingleton<T> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        Iterable<? extends WindowedValue<?>> iter =
                context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static <T> TransformEvaluator<View.AsIterable<T>> viewAsIter() {
    return new TransformEvaluator<View.AsIterable<T>>() {
      @Override
      public void evaluate(View.AsIterable<T> transform, EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        Iterable<? extends WindowedValue<?>> iter =
                context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static <R, W> TransformEvaluator<View.CreatePCollectionView<R, W>> createPCollView() {
    return new TransformEvaluator<View.CreatePCollectionView<R, W>>() {
      @Override
      public void evaluate(View.CreatePCollectionView<R, W> transform,
          EvaluationResult ctxt) {
        DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
        Iterable<? extends WindowedValue<?>> iter =
            context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

//  private static final class TupleTagFilter<V>
//      implements Function<Tuple2<TupleTag<V>, WindowedValue<?>>, Boolean> {
//
//    private final TupleTag<V> tag;
//
//    private TupleTagFilter(TupleTag<V> tag) {
//      this.tag = tag;
//    }
//
//    @Override
//    public Boolean call(Tuple2<TupleTag<V>, WindowedValue<?>> input) {
//      return tag.equals(input._1());
//    }
//  }

  private static Map<TupleTag<?>, BroadcastHelper<?>> getSideInputs(
      List<PCollectionView<?>> views,
      EvaluationResult ctxt) {
    DatasetsEvaluationContext context = (DatasetsEvaluationContext) ctxt;
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

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(TextIO.Read.Bound.class, readText());
    EVALUATORS.put(TextIO.Write.Bound.class, writeText());
//    EVALUATORS.put(AvroIO.Read.Bound.class, readAvro());
//    EVALUATORS.put(AvroIO.Write.Bound.class, writeAvro());
//    EVALUATORS.put(HadoopIO.Read.Bound.class, readHadoop());
//    EVALUATORS.put(HadoopIO.Write.Bound.class, writeHadoop());
    EVALUATORS.put(ParDo.Bound.class, parDo());
//    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());
    EVALUATORS.put(GroupByKey.class, gbk());
    EVALUATORS.put(Combine.GroupedValues.class, grouped());
//    EVALUATORS.put(Combine.Globally.class, combineGlobally());
    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
    EVALUATORS.put(Create.Values.class, create());
    EVALUATORS.put(View.AsSingleton.class, viewAsSingleton());
    EVALUATORS.put(View.AsIterable.class, viewAsIter());
    EVALUATORS.put(View.CreatePCollectionView.class, createPCollView());
    EVALUATORS.put(Window.Bound.class, window());
  }

  public static <PT extends PTransform<?, ?>> TransformEvaluator<PT>
  getTransformEvaluator(Class<PT> clazz) {
    @SuppressWarnings("unchecked")
    TransformEvaluator<PT> transform = (TransformEvaluator<PT>) EVALUATORS.get(clazz);
    if (transform == null) {
      throw new IllegalStateException("No TransformEvaluator registered for " + clazz);
    }
    return transform;
  }

  /**
   * TranslatorRDD matches Beam transformation with the appropriate evaluator.
   */
  public static class Translator implements SparkPipelineTranslator{

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      return EVALUATORS.containsKey(clazz);
    }

    @Override
    public <PT extends PTransform<?, ?>> TransformEvaluator<PT> translate(Class<PT> clazz) {
      return getTransformEvaluator(clazz);
    }
  }
}
