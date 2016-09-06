package org.apache.beam.runners.spark.translation;

/**
 * Created by ansela on 8/31/16.
 */
public interface Dataset<T> {

  void cache();

  void action();

  void setName(String name);

}
