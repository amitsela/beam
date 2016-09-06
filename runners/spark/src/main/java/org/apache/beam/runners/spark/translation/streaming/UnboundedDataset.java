package org.apache.beam.runners.spark.translation.streaming;

import org.apache.beam.runners.spark.translation.Dataset;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * DStream holder Can also crate a DStream from a supplied queue of values, but mainly for
 * testing.
 */
public class UnboundedDataset<T> implements Dataset<T> {
  private final JavaDStream<WindowedValue<T>> dStream;

  public UnboundedDataset(JavaDStream<WindowedValue<T>> dStream) {
    this.dStream = dStream;
  }

  public JavaDStream<WindowedValue<T>> getDStream() {
    return dStream;
  }

  @Override
  public void cache() {
    dStream.cache();
  }

  @Override
  public void action() {
    dStream.foreachRDD(new VoidFunction<JavaRDD<WindowedValue<T>>>() {
      @Override
      public void call(JavaRDD<WindowedValue<T>> rdd) throws Exception {
        rdd.count();
      }
    });
  }

  @Override
  public void setName(String name) {
    // ignore
  }
}
