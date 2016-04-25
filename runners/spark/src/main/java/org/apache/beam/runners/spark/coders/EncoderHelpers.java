package org.apache.beam.runners.spark.coders;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 * {@link org.apache.spark.sql.Encoders} utility class - currently only kryo
 * provides a solid support for nested, generically typed, classes.
 *
 * Providing helper methods for some commonly used encoding in the Spark runner.
 */
public final class EncoderHelpers {

  @SuppressWarnings("unchecked")
  public static <T> Encoder<T> valueEncoder() {
    return Encoders.kryo((Class<T>) Object.class);
  }

  @SuppressWarnings("unchecked")
  public static <T> Encoder<WindowedValue<T>> windowedValueEncoder() {
    return Encoders.kryo((Class<WindowedValue<T>>) (Class<?>) WindowedValue.class);
  }

  @SuppressWarnings("unchecked")
  public static <K, V> Encoder<KV<K, V>> kvEncoder() {
    return Encoders.kryo((Class<KV<K, V>>) (Class<?>) WindowedValue.class);
  }
}
