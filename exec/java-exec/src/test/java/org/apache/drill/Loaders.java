package org.apache.drill;

public final class Loaders {
  private Loaders(){}

  public static interface VarCharLoader {
    public long getStartEnd();
    public long getDataAddr();
  }

  public static interface NullableVarCharLoader extends VarCharLoader {
    public int getIsSet();
  }

  public static interface VarCharStorer {
    public void set(int isSet, )
  }

}
