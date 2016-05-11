package org.apache.beam.runners.spark.coders;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;

import scala.reflect.ClassTag;

/**
 * {@link org.apache.spark.sql.Encoders} utility class - currently only kryo
 * provides a solid support for nested, generically typed, classes.
 *
 * //TODO: write custom Encoders for common, generically typed, Beam Classes such as
 * {@link org.apache.beam.sdk.util.WindowedValue}, {@link org.apache.beam.sdk.values.KV}
 */
public final class EncoderHelpers {

  @SuppressWarnings("unchecked")
  public static <T> Encoder<T> kryoEncode() {
    return Encoders.kryo((Class<T>) Object.class);
  }

  @SuppressWarnings("unchecked")
  public static <V> Encoder<V> encodeValue() {
    return Encoders.bean((Class<V>) Object.class);
  }

  @SuppressWarnings("unchecked")
  public static <V> Encoder<WindowedValue<V>> encodeWindowedValue() {
    return new Encoder<WindowedValue<V>>() {

      @Override
      public StructType schema() {
        return null;
      }

      @Override
      public ClassTag<WindowedValue<V>> clsTag() {
        return null;
      }
    };
  }

}
