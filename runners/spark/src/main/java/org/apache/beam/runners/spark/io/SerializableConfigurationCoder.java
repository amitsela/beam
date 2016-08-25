package org.apache.beam.runners.spark.io;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.util.SerializableConfiguration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} for coding a {@link SerializableConfiguration}.
 */
public class SerializableConfigurationCoder extends AtomicCoder<SerializableConfiguration> {

  public static SerializableConfigurationCoder of() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final SerializableConfigurationCoder INSTANCE =
       new SerializableConfigurationCoder();

  private SerializableConfigurationCoder() {}

  @Override
  public void encode(SerializableConfiguration value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null SerializableConfiguration.");
    }
    value.value().write(new DataOutputStream(outStream));
  }

  @Override
  public SerializableConfiguration decode(InputStream inStream, Context context)
      throws CoderException, IOException {
    Configuration value = new Configuration(false);
    try {
      value.readFields(new DataInputStream(inStream));
      return new SerializableConfiguration(value);
    } catch (StreamCorruptedException | SecurityException e) {
      // These exceptions correspond to decoding problems, so change
      // what kind of exception they're branded as.
      throw new CoderException(e);
    }
  }
}
