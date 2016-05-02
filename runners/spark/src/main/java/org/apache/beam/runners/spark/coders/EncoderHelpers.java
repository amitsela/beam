package org.apache.beam.runners.spark.coders;

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
  public static <T> Encoder<T> encode() {
    return Encoders.kryo((Class<T>) Object.class);
  }
}
