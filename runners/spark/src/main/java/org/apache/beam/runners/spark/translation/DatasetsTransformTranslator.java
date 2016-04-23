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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.hadoop.HadoopIO;
import org.apache.beam.runners.spark.io.hadoop.ShardNameTemplateHelper;
import org.apache.beam.runners.spark.io.hadoop.TemplatedAvroKeyOutputFormat;
import org.apache.beam.runners.spark.io.hadoop.TemplatedTextOutputFormat;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.AssignWindowsDoFn;
import org.apache.beam.sdk.util.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.io.IOException;
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

  private static <T> DatasetsTransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new DatasetsTransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform,
          DatasetsEvaluationContext context) {
        PCollectionList<T> pcs = context.getInput(transform);
        JavaRDD<WindowedValue<T>>[] rdds = new JavaRDD[pcs.size()];
        for (int i = 0; i < rdds.length; i++) {
          rdds[i] = (JavaRDD<WindowedValue<T>>) context.getDataset(pcs.get(i));
        }
        JavaRDD<WindowedValue<T>> rdd = context.getSparkContext().union(rdds);
        context.setOutputDataset(transform, rdd);
      }
    };
  }

  private static <K, V> DatasetsTransformEvaluator<GroupByKeyOnly<K, V>> gbk() {
    return new DatasetsTransformEvaluator<GroupByKeyOnly<K, V>>() {
      @Override
      public void evaluate(GroupByKeyOnly<K, V> transform, DatasetsEvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, V>>, ?> inRDD =
            (JavaRDDLike<WindowedValue<KV<K, V>>, ?>) context.getInputDataset(transform);
        @SuppressWarnings("unchecked")
        KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        Coder<K> keyCoder = coder.getKeyCoder();
        Coder<V> valueCoder = coder.getValueCoder();

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaRDDLike<WindowedValue<KV<K, Iterable<V>>>, ?> outRDD = fromPair(
              toPair(inRDD.map(WindowingHelpers.<KV<K, V>>unwindowFunction()))
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, valueCoder))
            .groupByKey()
            .mapToPair(CoderHelpers.fromByteFunctionIterable(keyCoder, valueCoder)))
            // empty windows are OK here, see GroupByKey#evaluateHelper in the SDK
            .map(WindowingHelpers.<KV<K, Iterable<V>>>windowFunction());
        context.setOutputDataset(transform, outRDD);
      }
    };
  }

  private static final FieldGetter GROUPED_FG = new FieldGetter(Combine.GroupedValues.class);

  private static <K, VI, VO> DatasetsTransformEvaluator<Combine.GroupedValues<K, VI, VO>> grouped() {
    return new DatasetsTransformEvaluator<Combine.GroupedValues<K, VI, VO>>() {
      @Override
      public void evaluate(Combine.GroupedValues<K, VI, VO> transform,
          DatasetsEvaluationContext context) {
        Combine.KeyedCombineFn<K, VI, ?, VO> keyed = GROUPED_FG.get("fn", transform);
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, Iterable<VI>>>, ?> inRDD =
            (JavaRDDLike<WindowedValue<KV<K, Iterable<VI>>>, ?>) context.getInputDataset(transform);
        context.setOutputDataset(transform,
            inRDD.map(new KVFunction<>(keyed)));
      }
    };
  }

  private static final FieldGetter COMBINE_GLOBALLY_FG = new FieldGetter(Combine.Globally.class);

  private static <I, A, O> DatasetsTransformEvaluator<Combine.Globally<I, O>> combineGlobally() {
    return new DatasetsTransformEvaluator<Combine.Globally<I, O>>() {

      @Override
      public void evaluate(Combine.Globally<I, O> transform, DatasetsEvaluationContext context) {
        final Combine.CombineFn<I, A, O> globally = COMBINE_GLOBALLY_FG.get("fn", transform);

        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<I>, ?> inRdd =
            (JavaRDDLike<WindowedValue<I>, ?>) context.getInputDataset(transform);

        final Coder<I> iCoder = context.getInput(transform).getCoder();
        final Coder<A> aCoder;
        try {
          aCoder = globally.getAccumulatorCoder(
              context.getPipeline().getCoderRegistry(), iCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaRDD<byte[]> inRddBytes = inRdd
            .map(WindowingHelpers.<I>unwindowFunction())
            .map(CoderHelpers.toByteFunction(iCoder));

        /*A*/ byte[] acc = inRddBytes.aggregate(
            CoderHelpers.toByteArray(globally.createAccumulator(), aCoder),
            new Function2</*A*/ byte[], /*I*/ byte[], /*A*/ byte[]>() {
              @Override
              public /*A*/ byte[] call(/*A*/ byte[] ab, /*I*/ byte[] ib) throws Exception {
                A a = CoderHelpers.fromByteArray(ab, aCoder);
                I i = CoderHelpers.fromByteArray(ib, iCoder);
                return CoderHelpers.toByteArray(globally.addInput(a, i), aCoder);
              }
            },
            new Function2</*A*/ byte[], /*A*/ byte[], /*A*/ byte[]>() {
              @Override
              public /*A*/ byte[] call(/*A*/ byte[] a1b, /*A*/ byte[] a2b) throws Exception {
                A a1 = CoderHelpers.fromByteArray(a1b, aCoder);
                A a2 = CoderHelpers.fromByteArray(a2b, aCoder);
                // don't use Guava's ImmutableList.of as values may be null
                List<A> accumulators = Collections.unmodifiableList(Arrays.asList(a1, a2));
                A merged = globally.mergeAccumulators(accumulators);
                return CoderHelpers.toByteArray(merged, aCoder);
              }
            }
        );
        O output = globally.extractOutput(CoderHelpers.fromByteArray(acc, aCoder));

        Coder<O> coder = context.getOutput(transform).getCoder();
        JavaRDD<byte[]> outRdd = context.getSparkContext().parallelize(
            // don't use Guava's ImmutableList.of as output may be null
            CoderHelpers.toByteArrays(Collections.singleton(output), coder));
        context.setOutputDataset(transform, outRdd.map(CoderHelpers.fromByteFunction(coder))
            .map(WindowingHelpers.<O>windowFunction()));
      }
    };
  }

  private static final FieldGetter COMBINE_PERKEY_FG = new FieldGetter(Combine.PerKey.class);

  private static <K, VI, VA, VO> DatasetsTransformEvaluator<Combine.PerKey<K, VI, VO>> combinePerKey() {
    return new DatasetsTransformEvaluator<Combine.PerKey<K, VI, VO>>() {
      @Override
      public void evaluate(Combine.PerKey<K, VI, VO> transform, DatasetsEvaluationContext context) {
        final Combine.KeyedCombineFn<K, VI, VA, VO> keyed =
            COMBINE_PERKEY_FG.get("fn", transform);
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<KV<K, VI>>, ?> inRdd =
            (JavaRDDLike<WindowedValue<KV<K, VI>>, ?>) context.getInputDataset(transform);

        @SuppressWarnings("unchecked")
        KvCoder<K, VI> inputCoder = (KvCoder<K, VI>) context.getInput(transform).getCoder();
        Coder<K> keyCoder = inputCoder.getKeyCoder();
        Coder<VI> viCoder = inputCoder.getValueCoder();
        Coder<VA> vaCoder;
        try {
          vaCoder = keyed.getAccumulatorCoder(
              context.getPipeline().getCoderRegistry(), keyCoder, viCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }
        Coder<KV<K, VI>> kviCoder = KvCoder.of(keyCoder, viCoder);
        Coder<KV<K, VA>> kvaCoder = KvCoder.of(keyCoder, vaCoder);

        // We need to duplicate K as both the key of the JavaPairRDD as well as inside the value,
        // since the functions passed to combineByKey don't receive the associated key of each
        // value, and we need to map back into methods in Combine.KeyedCombineFn, which each
        // require the key in addition to the VI's and VA's being merged/accumulated. Once Spark
        // provides a way to include keys in the arguments of combine/merge functions, we won't
        // need to duplicate the keys anymore.

        // Key has to bw windowed in order to group by window as well
        JavaPairRDD<WindowedValue<K>, WindowedValue<KV<K, VI>>> inRddDuplicatedKeyPair =
            inRdd.flatMapToPair(
                new PairFlatMapFunction<WindowedValue<KV<K, VI>>, WindowedValue<K>,
                    WindowedValue<KV<K, VI>>>() {
                  @Override
                  public Iterable<Tuple2<WindowedValue<K>,
                      WindowedValue<KV<K, VI>>>> call(WindowedValue<KV<K, VI>> kv) {
                      List<Tuple2<WindowedValue<K>,
                          WindowedValue<KV<K, VI>>>> tuple2s =
                          Lists.newArrayListWithCapacity(kv.getWindows().size());
                      for (BoundedWindow boundedWindow: kv.getWindows()) {
                        WindowedValue<K> wk = WindowedValue.of(kv.getValue().getKey(),
                            boundedWindow.maxTimestamp(), boundedWindow, kv.getPane());
                        tuple2s.add(new Tuple2<>(wk, kv));
                      }
                    return tuple2s;
                  }
                });
        //-- windowed coders
        final WindowedValue.FullWindowedValueCoder<K> wkCoder =
                WindowedValue.FullWindowedValueCoder.of(keyCoder,
                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());
        final WindowedValue.FullWindowedValueCoder<KV<K, VI>> wkviCoder =
                WindowedValue.FullWindowedValueCoder.of(kviCoder,
                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());
        final WindowedValue.FullWindowedValueCoder<KV<K, VA>> wkvaCoder =
                WindowedValue.FullWindowedValueCoder.of(kvaCoder,
                context.getInput(transform).getWindowingStrategy().getWindowFn().windowCoder());

        // Use coders to convert objects in the PCollection to byte arrays, so they
        // can be transferred over the network for the shuffle.
        JavaPairRDD<ByteArray, byte[]> inRddDuplicatedKeyPairBytes = inRddDuplicatedKeyPair
            .mapToPair(CoderHelpers.toByteFunction(wkCoder, wkviCoder));

        // The output of combineByKey will be "VA" (accumulator) types rather than "VO" (final
        // output types) since Combine.CombineFn only provides ways to merge VAs, and no way
        // to merge VOs.
        JavaPairRDD</*K*/ ByteArray, /*KV<K, VA>*/ byte[]> accumulatedBytes =
            inRddDuplicatedKeyPairBytes.combineByKey(
            new Function</*KV<K, VI>*/ byte[], /*KV<K, VA>*/ byte[]>() {
              @Override
              public /*KV<K, VA>*/ byte[] call(/*KV<K, VI>*/ byte[] input) {
                WindowedValue<KV<K, VI>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
                VA va = keyed.createAccumulator(wkvi.getValue().getKey());
                va = keyed.addInput(wkvi.getValue().getKey(), va, wkvi.getValue().getValue());
                WindowedValue<KV<K, VA>> wkva =
                    WindowedValue.of(KV.of(wkvi.getValue().getKey(), va), wkvi.getTimestamp(),
                    wkvi.getWindows(), wkvi.getPane());
                return CoderHelpers.toByteArray(wkva, wkvaCoder);
              }
            },
            new Function2</*KV<K, VA>*/ byte[], /*KV<K, VI>*/ byte[], /*KV<K, VA>*/ byte[]>() {
              @Override
              public /*KV<K, VA>*/ byte[] call(/*KV<K, VA>*/ byte[] acc,
                  /*KV<K, VI>*/ byte[] input) {
                WindowedValue<KV<K, VA>> wkva = CoderHelpers.fromByteArray(acc, wkvaCoder);
                WindowedValue<KV<K, VI>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
                VA va = keyed.addInput(wkva.getValue().getKey(), wkva.getValue().getValue(),
                    wkvi.getValue().getValue());
                wkva = WindowedValue.of(KV.of(wkva.getValue().getKey(), va), wkva.getTimestamp(),
                    wkva.getWindows(), wkva.getPane());
                return CoderHelpers.toByteArray(wkva, wkvaCoder);
              }
            },
            new Function2</*KV<K, VA>*/ byte[], /*KV<K, VA>*/ byte[], /*KV<K, VA>*/ byte[]>() {
              @Override
              public /*KV<K, VA>*/ byte[] call(/*KV<K, VA>*/ byte[] acc1,
                  /*KV<K, VA>*/ byte[] acc2) {
                WindowedValue<KV<K, VA>> wkva1 = CoderHelpers.fromByteArray(acc1, wkvaCoder);
                WindowedValue<KV<K, VA>> wkva2 = CoderHelpers.fromByteArray(acc2, wkvaCoder);
                VA va = keyed.mergeAccumulators(wkva1.getValue().getKey(),
                    // don't use Guava's ImmutableList.of as values may be null
                    Collections.unmodifiableList(Arrays.asList(wkva1.getValue().getValue(),
                    wkva2.getValue().getValue())));
                WindowedValue<KV<K, VA>> wkva = WindowedValue.of(KV.of(wkva1.getValue().getKey(),
                    va), wkva1.getTimestamp(), wkva1.getWindows(), wkva1.getPane());
                return CoderHelpers.toByteArray(wkva, wkvaCoder);
              }
            });

        JavaPairRDD<WindowedValue<K>, WindowedValue<VO>> extracted = accumulatedBytes
            .mapToPair(CoderHelpers.fromByteFunction(wkCoder, wkvaCoder))
            .mapValues(
                new Function<WindowedValue<KV<K, VA>>, WindowedValue<VO>>() {
                  @Override
                  public WindowedValue<VO> call(WindowedValue<KV<K, VA>> acc) {
                    return WindowedValue.of(keyed.extractOutput(acc.getValue().getKey(),
                        acc.getValue().getValue()), acc.getTimestamp(),
                        acc.getWindows(), acc.getPane());
                  }
                });

        context.setOutputDataset(transform,
            fromPair(extracted)
            .map(new Function<KV<WindowedValue<K>, WindowedValue<VO>>, WindowedValue<KV<K, VO>>>() {
              @Override
              public WindowedValue<KV<K, VO>> call(KV<WindowedValue<K>, WindowedValue<VO>> kwvo)
                  throws Exception {
                WindowedValue<VO> wvo = kwvo.getValue();
                KV<K, VO> kvo = KV.of(kwvo.getKey().getValue(), wvo.getValue());
                return WindowedValue.of(kvo, wvo.getTimestamp(), wvo.getWindows(), wvo.getPane());
              }
            }));
      }
    };
  }

  private static final class KVFunction<K, VI, VO>
      implements Function<WindowedValue<KV<K, Iterable<VI>>>, WindowedValue<KV<K, VO>>> {
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

  private static <I, O> DatasetsTransformEvaluator<ParDo.Bound<I, O>> parDo() {
    return new DatasetsTransformEvaluator<ParDo.Bound<I, O>>() {
      @Override
      public void evaluate(ParDo.Bound<I, O> transform, DatasetsEvaluationContext context) {
        DoFnFunction<I, O> dofn =
            new DoFnFunction<>(transform.getFn(),
                context.getRuntimeContext(),
                getSideInputs(transform.getSideInputs(), context));
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<I>, ?> inRDD =
            (JavaRDDLike<WindowedValue<I>, ?>) context.getInputDataset(transform);
        context.setOutputDataset(transform, inRDD.mapPartitions(dofn));
      }
    };
  }

  private static final FieldGetter MULTIDO_FG = new FieldGetter(ParDo.BoundMulti.class);

  private static <I, O> DatasetsTransformEvaluator<ParDo.BoundMulti<I, O>> multiDo() {
    return new DatasetsTransformEvaluator<ParDo.BoundMulti<I, O>>() {
      @Override
      public void evaluate(ParDo.BoundMulti<I, O> transform, DatasetsEvaluationContext context) {
        TupleTag<O> mainOutputTag = MULTIDO_FG.get("mainOutputTag", transform);
        MultiDoFnFunction<I, O> multifn = new MultiDoFnFunction<>(
            transform.getFn(),
            context.getRuntimeContext(),
            mainOutputTag,
            getSideInputs(transform.getSideInputs(), context));

        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<I>, ?> inRDD =
            (JavaRDDLike<WindowedValue<I>, ?>) context.getInputDataset(transform);
        JavaPairRDD<TupleTag<?>, WindowedValue<?>> all = inRDD
            .mapPartitionsToPair(multifn)
            .cache();

        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairRDD<TupleTag<?>, WindowedValue<?>> filtered =
              all.filter(new TupleTagFilter(e.getKey()));
          @SuppressWarnings("unchecked")
          // Object is the best we can do since different outputs can have different tags
          JavaRDD<WindowedValue<Object>> values =
              (JavaRDD<WindowedValue<Object>>) (JavaRDD<?>) filtered.values();
          context.setDataset(e.getValue(), values);
        }
      }
    };
  }


  private static <T> DatasetsTransformEvaluator<TextIO.Read.Bound<T>> readText() {
    return new DatasetsTransformEvaluator<TextIO.Read.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Read.Bound<T> transform, DatasetsEvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaRDD<WindowedValue<String>> rdd = context.getSparkContext().textFile(pattern)
                .map(WindowingHelpers.<String>windowFunction());
        context.setOutputDataset(transform, rdd);
      }
    };
  }

  private static <T> DatasetsTransformEvaluator<TextIO.Write.Bound<T>> writeText() {
    return new DatasetsTransformEvaluator<TextIO.Write.Bound<T>>() {
      @Override
      public void evaluate(TextIO.Write.Bound<T> transform, DatasetsEvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<T, Void> last =
            ((JavaRDDLike<WindowedValue<T>, ?>) context.getInputDataset(transform))
            .map(WindowingHelpers.<T>unwindowFunction())
            .mapToPair(new PairFunction<T, T,
                    Void>() {
              @Override
              public Tuple2<T, Void> call(T t) throws Exception {
                return new Tuple2<>(t, null);
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
                transform.getShardTemplate(), transform.getFilenamePrefix(),
                transform.getFilenameSuffix());
        writeHadoopFile(last, new Configuration(), shardTemplateInfo, Text.class,
            NullWritable.class, TemplatedTextOutputFormat.class);
      }
    };
  }

  private static <T> DatasetsTransformEvaluator<AvroIO.Read.Bound<T>> readAvro() {
    return new DatasetsTransformEvaluator<AvroIO.Read.Bound<T>>() {
      @Override
      public void evaluate(AvroIO.Read.Bound<T> transform, DatasetsEvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaSparkContext jsc = context.getSparkContext();
        @SuppressWarnings("unchecked")
        JavaRDD<AvroKey<T>> avroFile = (JavaRDD<AvroKey<T>>) (JavaRDD<?>)
            jsc.newAPIHadoopFile(pattern,
                                 AvroKeyInputFormat.class,
                                 AvroKey.class, NullWritable.class,
                                 new Configuration()).keys();
        JavaRDD<WindowedValue<T>> rdd = avroFile.map(
            new Function<AvroKey<T>, T>() {
              @Override
              public T call(AvroKey<T> key) {
                return key.datum();
              }
            }).map(WindowingHelpers.<T>windowFunction());
        context.setOutputDataset(transform, rdd);
      }
    };
  }

  private static <T> DatasetsTransformEvaluator<AvroIO.Write.Bound<T>> writeAvro() {
    return new DatasetsTransformEvaluator<AvroIO.Write.Bound<T>>() {
      @Override
      public void evaluate(AvroIO.Write.Bound<T> transform, DatasetsEvaluationContext context) {
        Job job;
        try {
          job = Job.getInstance();
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        AvroJob.setOutputKeySchema(job, transform.getSchema());
        @SuppressWarnings("unchecked")
        JavaPairRDD<AvroKey<T>, NullWritable> last =
            ((JavaRDDLike<WindowedValue<T>, ?>) context.getInputDataset(transform))
            .map(WindowingHelpers.<T>unwindowFunction())
            .mapToPair(new PairFunction<T, AvroKey<T>, NullWritable>() {
              @Override
              public Tuple2<AvroKey<T>, NullWritable> call(T t) throws Exception {
                return new Tuple2<>(new AvroKey<>(t), NullWritable.get());
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
            transform.getShardTemplate(), transform.getFilenamePrefix(),
            transform.getFilenameSuffix());
        writeHadoopFile(last, job.getConfiguration(), shardTemplateInfo,
            AvroKey.class, NullWritable.class, TemplatedAvroKeyOutputFormat.class);
      }
    };
  }

  private static <K, V> DatasetsTransformEvaluator<HadoopIO.Read.Bound<K, V>> readHadoop() {
    return new DatasetsTransformEvaluator<HadoopIO.Read.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Read.Bound<K, V> transform, DatasetsEvaluationContext context) {
        String pattern = transform.getFilepattern();
        JavaSparkContext jsc = context.getSparkContext();
        @SuppressWarnings ("unchecked")
        JavaPairRDD<K, V> file = jsc.newAPIHadoopFile(pattern,
            transform.getFormatClass(),
            transform.getKeyClass(), transform.getValueClass(),
            new Configuration());
        JavaRDD<WindowedValue<KV<K, V>>> rdd =
            file.map(new Function<Tuple2<K, V>, KV<K, V>>() {
          @Override
          public KV<K, V> call(Tuple2<K, V> t2) throws Exception {
            return KV.of(t2._1(), t2._2());
          }
        }).map(WindowingHelpers.<KV<K, V>>windowFunction());
        context.setOutputDataset(transform, rdd);
      }
    };
  }

  private static <K, V> DatasetsTransformEvaluator<HadoopIO.Write.Bound<K, V>> writeHadoop() {
    return new DatasetsTransformEvaluator<HadoopIO.Write.Bound<K, V>>() {
      @Override
      public void evaluate(HadoopIO.Write.Bound<K, V> transform, DatasetsEvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaPairRDD<K, V> last = ((JavaRDDLike<WindowedValue<KV<K, V>>, ?>) context
            .getInputDataset(transform))
            .map(WindowingHelpers.<KV<K, V>>unwindowFunction())
            .mapToPair(new PairFunction<KV<K, V>, K, V>() {
              @Override
              public Tuple2<K, V> call(KV<K, V> t) throws Exception {
                return new Tuple2<>(t.getKey(), t.getValue());
              }
            });
        ShardTemplateInformation shardTemplateInfo =
            new ShardTemplateInformation(transform.getNumShards(),
                transform.getShardTemplate(), transform.getFilenamePrefix(),
                transform.getFilenameSuffix());
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> e : transform.getConfigurationProperties().entrySet()) {
          conf.set(e.getKey(), e.getValue());
        }
        writeHadoopFile(last, conf, shardTemplateInfo,
            transform.getKeyClass(), transform.getValueClass(), transform.getFormatClass());
      }
    };
  }

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

  private static <K, V> void writeHadoopFile(JavaPairRDD<K, V> rdd, Configuration conf,
      ShardTemplateInformation shardTemplateInfo, Class<?> keyClass, Class<?> valueClass,
      Class<? extends FileOutputFormat> formatClass) {
    int numShards = shardTemplateInfo.getNumShards();
    String shardTemplate = shardTemplateInfo.getShardTemplate();
    String filenamePrefix = shardTemplateInfo.getFilenamePrefix();
    String filenameSuffix = shardTemplateInfo.getFilenameSuffix();
    if (numShards != 0) {
      // number of shards was set explicitly, so repartition
      rdd = rdd.repartition(numShards);
    }
    int actualNumShards = rdd.partitions().size();
    String template = replaceShardCount(shardTemplate, actualNumShards);
    String outputDir = getOutputDirectory(filenamePrefix, template);
    String filePrefix = getOutputFilePrefix(filenamePrefix, template);
    String fileTemplate = getOutputFileTemplate(filenamePrefix, template);

    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_PREFIX, filePrefix);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_TEMPLATE, fileTemplate);
    conf.set(ShardNameTemplateHelper.OUTPUT_FILE_SUFFIX, filenameSuffix);
    rdd.saveAsNewAPIHadoopFile(outputDir, keyClass, valueClass, formatClass, conf);
  }

  private static final FieldGetter WINDOW_FG = new FieldGetter(Window.Bound.class);

  private static <T, W extends BoundedWindow> DatasetsTransformEvaluator<Window.Bound<T>> window() {
    return new DatasetsTransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, DatasetsEvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDDLike<WindowedValue<T>, ?> inRDD =
            (JavaRDDLike<WindowedValue<T>, ?>) context.getInputDataset(transform);
        WindowFn<? super T, W> windowFn = WINDOW_FG.get("windowFn", transform);
        if (windowFn instanceof GlobalWindows) {
          context.setOutputDataset(transform, inRDD);
        } else {
          @SuppressWarnings("unchecked")
          DoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          DoFnFunction<T, T> dofn =
                  new DoFnFunction<>(addWindowsDoFn, context.getRuntimeContext(), null);
          context.setOutputDataset(transform, inRDD.mapPartitions(dofn));
        }
      }
    };
  }

  private static <T> DatasetsTransformEvaluator<Create.Values<T>> create() {
    return new DatasetsTransformEvaluator<Create.Values<T>>() {
      @Override
      public void evaluate(Create.Values<T> transform, DatasetsEvaluationContext context) {
        Iterable<T> elems = transform.getElements();
        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = context.getOutput(transform).getCoder();
        context.setOutputDatasetFromValues(transform, elems, coder);
      }
    };
  }

  private static <T> DatasetsTransformEvaluator<View.AsSingleton<T>> viewAsSingleton() {
    return new DatasetsTransformEvaluator<View.AsSingleton<T>>() {
      @Override
      public void evaluate(View.AsSingleton<T> transform, DatasetsEvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
                context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static <T> DatasetsTransformEvaluator<View.AsIterable<T>> viewAsIter() {
    return new DatasetsTransformEvaluator<View.AsIterable<T>>() {
      @Override
      public void evaluate(View.AsIterable<T> transform, DatasetsEvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
                context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static <R, W> DatasetsTransformEvaluator<View.CreatePCollectionView<R, W>> createPCollView() {
    return new DatasetsTransformEvaluator<View.CreatePCollectionView<R, W>>() {
      @Override
      public void evaluate(View.CreatePCollectionView<R, W> transform,
          DatasetsEvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
            context.getWindowedValues(context.getInput(transform));
        context.setPView(context.getOutput(transform), iter);
      }
    };
  }

  private static final class TupleTagFilter<V>
      implements Function<Tuple2<TupleTag<V>, WindowedValue<?>>, Boolean> {

    private final TupleTag<V> tag;

    private TupleTagFilter(TupleTag<V> tag) {
      this.tag = tag;
    }

    @Override
    public Boolean call(Tuple2<TupleTag<V>, WindowedValue<?>> input) {
      return tag.equals(input._1());
    }
  }

  private static Map<TupleTag<?>, BroadcastHelper<?>> getSideInputs(
      List<PCollectionView<?>> views,
      DatasetsEvaluationContext context) {
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

  private static final Map<Class<? extends PTransform>, DatasetsTransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(TextIO.Read.Bound.class, readText());
    EVALUATORS.put(TextIO.Write.Bound.class, writeText());
    EVALUATORS.put(AvroIO.Read.Bound.class, readAvro());
    EVALUATORS.put(AvroIO.Write.Bound.class, writeAvro());
    EVALUATORS.put(HadoopIO.Read.Bound.class, readHadoop());
    EVALUATORS.put(HadoopIO.Write.Bound.class, writeHadoop());
    EVALUATORS.put(ParDo.Bound.class, parDo());
    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());
    EVALUATORS.put(GroupByKeyOnly.class, gbk());
    EVALUATORS.put(Combine.GroupedValues.class, grouped());
    EVALUATORS.put(Combine.Globally.class, combineGlobally());
    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
    EVALUATORS.put(Create.Values.class, create());
    EVALUATORS.put(View.AsSingleton.class, viewAsSingleton());
    EVALUATORS.put(View.AsIterable.class, viewAsIter());
    EVALUATORS.put(View.CreatePCollectionView.class, createPCollView());
    EVALUATORS.put(Window.Bound.class, window());
  }

  public static <PT extends PTransform<?, ?>> DatasetsTransformEvaluator<PT>
  getTransformEvaluator(Class<PT> clazz) {
    @SuppressWarnings("unchecked")
    DatasetsTransformEvaluator<PT> transform = (DatasetsTransformEvaluator<PT>) EVALUATORS.get(clazz);
    if (transform == null) {
      throw new IllegalStateException("No DatasetsTransformEvaluator registered for " + clazz);
    }
    return transform;
  }

  /**
   * TranslatorRDD matches Beam transformation with the appropriate evaluator.
   */
  public static class Translator implements SparkDatasetsPipelineTranslator{

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      return EVALUATORS.containsKey(clazz);
    }

    @Override
    public <PT extends PTransform<?, ?>> DatasetsTransformEvaluator<PT> translate(Class<PT> clazz) {
      return getTransformEvaluator(clazz);
    }
  }
}
