package org.apache.beam.runners.spark.translation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;

import java.util.List;

/**
 * Holds an RDD or values for deferred conversion to an RDD if needed. PCollections are
 * sometimes created from a collection of objects (using RDD parallelize) and then
 * only used to create View objects; in which case they do not need to be
 * converted to bytes since they are not transferred across the network until they are
 * broadcast.
 */
public class BoundedDataset<T> implements Dataset<T> {
  private final JavaRDD<WindowedValue<T>> rdd;

  BoundedDataset(JavaRDD<WindowedValue<T>> rdd) {
    this.rdd = rdd;
  }

  public JavaRDD<WindowedValue<T>> getRDD() {
    return rdd;
  }

  Iterable<WindowedValue<T>> getValues(PCollection<T> pcollection) {
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
      JavaRDDLike<byte[], ?> bytesRDD =
          rdd.map(CoderHelpers.toByteFunction(windowedValueCoder));
      List<byte[]> clientBytes = bytesRDD.collect();
      return Iterables.transform(clientBytes,
          new Function<byte[], WindowedValue<T>>() {
        @Override
        public WindowedValue<T> apply(byte[] bytes) {
          return CoderHelpers.fromByteArray(bytes, windowedValueCoder);
        }
      });
  }

  @Override
  public void cache() {
    rdd.cache();
  }

  @Override
  public void action() {
    rdd.count();
  }

  @Override
  public void setName(String name) {
    rdd.setName(name);
  }

}
