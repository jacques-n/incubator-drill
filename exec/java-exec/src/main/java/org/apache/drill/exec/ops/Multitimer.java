package org.apache.drill.exec.ops;

import org.slf4j.Logger;

public class Multitimer<T extends Enum<T>> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Multitimer.class);

  private final long start;
  private final long[] times;
  private final Class<T> clazz;

  public Multitimer(Class<T> clazz){
    this.times = new long[clazz.getEnumConstants().length];
    this.start = System.nanoTime();
    this.clazz = clazz;
  }

  public void mark(T timer){
    times[timer.ordinal()] = System.nanoTime();
  }

  public void log(Logger logger){

  }
}
