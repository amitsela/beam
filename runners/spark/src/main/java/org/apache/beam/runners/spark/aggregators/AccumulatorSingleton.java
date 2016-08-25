package org.apache.beam.runners.spark.aggregators;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * For resilience, {@link Accumulator}s are required to be wrapped in a Singleton.
 * For more details see: https://spark.apache.org/docs/1.6.2/streaming-programming-guide.html#accumulators-and-broadcast-variables
 */
public class AccumulatorSingleton {

  private static volatile Accumulator<NamedAggregators> instance = null;

  public static Accumulator<NamedAggregators> getInstance(JavaSparkContext jsc) {
    if (instance == null) {
      synchronized (AccumulatorSingleton.class) {
        if (instance == null) {
          instance = jsc.sc().accumulator(new NamedAggregators(), new AggAccumParam());
        }
      }
    }
    return instance;
  }

  /** For testing only. */
  public static void clear() {
    synchronized (AccumulatorSingleton.class) {
      instance = null;
    }
  }
}
