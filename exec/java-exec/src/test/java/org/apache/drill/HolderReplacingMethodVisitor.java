package org.apache.drill;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.LocalVariablesSorter;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class HolderReplacingMethodVisitor extends MethodVisitor {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HolderReplacingMethodVisitor.class);

  private final LocalVariablesSorter adder;
  private final IntObjectOpenHashMap<ValueHolderIden.ValueHolderSub> oldToNew = new IntObjectOpenHashMap<>();
  private ValueHolderIden.ValueHolderSub sub = null;
  private State state = State.NORMAL;
  private int lastLineNumber = 0;

  private static enum State {
    NORMAL, REPLACING_CREATE, REPLACING_REFERENCE
  }

  public HolderReplacingMethodVisitor(int access, String name, String desc, String signature, MethodVisitor mw) {
    super(Opcodes.ASM4, new LocalVariablesSorter(access, desc, mw));
    this.adder = (LocalVariablesSorter) this.mv;
  }

  @Override
  public void visitTypeInsn(int opcode, String type) {
    ValueHolderIden iden = HOLDERS.get(type);
    if (iden != null) {
      this.sub = iden.getHolderSub(adder);
      this.state = State.REPLACING_CREATE;
    } else {
      super.visitTypeInsn(opcode, type);
    }
  }

  @Override
  public void visitLineNumber(int line, Label start) {
    lastLineNumber = line;
    super.visitLineNumber(line, start);
  }

  @Override
  public void visitVarInsn(int opcode, int var) {
    switch(state){
    case REPLACING_REFERENCE:
    case NORMAL:
      if(oldToNew.containsKey(var)){
        sub = oldToNew.get(var);
        state = State.REPLACING_REFERENCE;
      }else{
        super.visitVarInsn(opcode, var);
      }
      break;
    case REPLACING_CREATE:
      oldToNew.put(var, sub);
      state = State.NORMAL;
      break;
    }
  }

  private void directVarInsn(int opcode, int var) {
    mv.visitVarInsn(opcode, var);
  }

  @Override
  public void visitFieldInsn(int opcode, String owner, String name, String desc) {
    switch(state){
    case NORMAL:
      super.visitFieldInsn(opcode, owner, name, desc);
      break;
    case REPLACING_CREATE:
      // noop.
      break;
    case REPLACING_REFERENCE:
      sub.addInsn(name, this, opcode);
      break;
    }
  }

  @Override
  public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
//    System.out.println("Local Var: " + desc);
    if (!HOLDER_DESCRIPTORS.contains(desc)) {
      super.visitLocalVariable(name, desc, signature, start, end, index);
    }
  }


  @Override
  public void visitInsn(int opcode) {
    if(state == State.REPLACING_CREATE && opcode == Opcodes.DUP){

    }else{
      super.visitInsn(opcode);
    }
  }

  @Override
  public void visitMethodInsn(int opcode, String owner, String name, String desc) {
    switch(state){
    case NORMAL:
      super.visitMethodInsn(opcode, owner, name, desc);
      break;
    case REPLACING_CREATE:
      // noop.
      break;
    case REPLACING_REFERENCE:
      throw new IllegalStateException(String.format("Holder types are not allowed to be passed between methods.  Ran across problem attempting to invoke method '%s' on line number %d", name, lastLineNumber));
    }
  }

  private static class ValueHolderIden {

    final ObjectIntOpenHashMap<String> fieldMap;
    final Type[] types;

    public ValueHolderIden(Class<?> c) {
      Field[] fields = c.getFields();
      List<Field> fldList = Lists.newArrayList();
      for(Field f : fields){
        if(!Modifier.isStatic(f.getModifiers())) {
          fldList.add(f);
        }
      }

      this.types = new Type[fldList.size()];
      fieldMap = new ObjectIntOpenHashMap<String>();
      int i =0;
      for(Field f : fldList){
        types[i] = Type.getType(f.getType());
        fieldMap.put(f.getName(), i);
        i++;
      }
    }

    public ValueHolderSub getHolderSub(LocalVariablesSorter adder) {
      int first = -1;
      for (int i = 0; i < types.length; i++) {
        int varIndex = adder.newLocal(types[i]);
        if (i == 0) {
          first = varIndex;
        }
      }

      return new ValueHolderSub(first);

    }

    private class ValueHolderSub {
      private final int first;

      public ValueHolderSub(int first) {
        assert first != -1 : "Create Holder for sub that doesn't have any fields.";
        this.first = first;
      }

      private int field(String name, HolderReplacingMethodVisitor mv) {
        if (!fieldMap.containsKey(name))
          throw new IllegalArgumentException(String.format("Unknown name '%s' on line %d.", name, mv.lastLineNumber));
        return fieldMap.lget();
      }

      public void addInsn(String name, HolderReplacingMethodVisitor mv, int opcode) {
        switch (opcode) {
        case Opcodes.GETFIELD:
          addKnownInsn(name, mv, Opcodes.ILOAD);
          return;

        case Opcodes.PUTFIELD:
          addKnownInsn(name, mv, Opcodes.ISTORE);
        }
      }

      private void addKnownInsn(String name, HolderReplacingMethodVisitor mv, int analogOpcode) {
        int f = field(name, mv);
        Type t = types[f];
        mv.directVarInsn(t.getOpcode(analogOpcode), first + f);
      }

    }


  }

  private static String desc(Class<?> c) {
    Type t = Type.getType(c);
    return t.getDescriptor();
  }

  static {
    Class<?>[] CLASSES = { //
    VarCharHolder.class, //
        NullableVarCharHolder.class, //
        NullableIntHolder.class //
    };

    ImmutableMap.Builder<String, ValueHolderIden> builder = ImmutableMap.builder();
    ImmutableSet.Builder<String> setB = ImmutableSet.builder();
    for (Class<?> c : CLASSES) {
      String desc = desc(c);
      setB.add(desc);
      desc = desc.substring(1, desc.length() - 1);
      System.out.println(desc);

      builder.put(desc, new ValueHolderIden(c));
    }
    HOLDER_DESCRIPTORS = setB.build();
    HOLDERS = builder.build();
  }

  private final static ImmutableMap<String, ValueHolderIden> HOLDERS;
  private final static ImmutableSet<String> HOLDER_DESCRIPTORS;

}
