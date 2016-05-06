package org.apache.beam.runners.spark.coders;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

/**
 * {@link org.apache.spark.sql.Encoders} utility class - currently only kryo
 * provides a solid support for nested, generically typed, classes.
 *
 * //TODO: write custom Encoders for common, generically typed, Beam Classes such as
 * {@link org.apache.beam.sdk.util.WindowedValue}, {@link org.apache.beam.sdk.values.KV}
 */
public final class EncoderHelpers {

  @SuppressWarnings("unchecked")
  public static <T> Encoder<T> encode() {
    return Encoders.kryo((Class<T>) Object.class);
  }
}
