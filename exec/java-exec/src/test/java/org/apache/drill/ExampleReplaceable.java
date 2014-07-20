package org.apache.drill;

import org.apache.drill.exec.expr.holders.NullableIntHolder;

public class ExampleReplaceable {

  public static void main(String[] args){
    ExampleReplaceable r = new ExampleReplaceable();
    System.out.println(r.xyz());
  }

  public static void z(NullableIntHolder h){

  }
  public int xyz(){

    NullableIntHolder h = new NullableIntHolder();
    z(h);
    NullableIntHolder h2 = new NullableIntHolder();
    h.isSet = 1;
    h.value = 4;
    h2.isSet = 1;
    h2.value = 6;
    if(h.isSet == h2.isSet){
      return h.value + h2.value;
    }else{
      return -1;
    }

  }
}
