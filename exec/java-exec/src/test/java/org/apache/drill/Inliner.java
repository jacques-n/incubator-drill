package org.apache.drill;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.hydromatic.linq4j.expressions.CatchBlock;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.LocalVariablesSorter;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingMethodAdapter;
import org.objectweb.asm.tree.MethodNode;

public class Inliner extends LocalVariablesSorter {
  private final String oldClass;
  private final String newClass;
  private final MethodNode mn;
  private List blocks = new ArrayList();
  private boolean inlining;

  private MethodCallInliner(int access, String desc, MethodVisitor mv, MethodNode mn, String oldClass, String newClass) {
    super(access, desc, mv);
    this.oldClass = oldClass;
    this.newClass = newClass;
    this.mn = mn;
  }

  public void visitMethodInsn(int opcode, String owner, String name, String desc) {
    if (!canBeInlined(owner, name, desc)) {
      mv.visitMethodInsn(opcode, owner, name, desc);
      return;
    }
    Map map = Collections.singletonMap(oldClass, newClass);
    Remapper remapper = new Remapper(map);
    Label end = new Label();
    inlining = true;
    mn.instructions.resetLabels();
    mn.accept(new InliningAdapter(this, opcode == Opcodes.INVOKESTATIC ? Opcodes.ACC_STATIC : 0, desc, remapper, end));
    inlining = false;
    super.visitLabel(end);
  }

  public void visitTryCatchBlock(Label start,
      Label end, Label handler, String type) {
    if(!inlining) {
      blocks.add(new CatchBlock(start, end, handler, type));
    } else {
      super.visitTryCatchBlock(start, end, handler, type);
    }
  }

  public void visitMaxs(int stack, int locals) {
    Iterator it = blocks.iterator();
    while (it.hasNext()) {
      CatchBlock b = (CatchBlock) it.next();
      super.visitTryCatchBlock(b.start, b.end, b.handler, b.type);
    }
    super.visitMaxs(stack, locals);
  }


  public static class InliningAdapter extends RemappingMethodAdapter {
    private final LocalVariablesSorter lvs;
    private final Label end;
    public InliningAdapter(LocalVariablesSorter mv,
          Label end, int acc, String desc,
          Remapper remapper) {
      super(acc, desc, mv, remapper);
      this.lvs = mv;
      this.end = end;
      int off = (acc & Opcodes.ACC_STATIC)!=0 ?
          0 : 1;
}
}