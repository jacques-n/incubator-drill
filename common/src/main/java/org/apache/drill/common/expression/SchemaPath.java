/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common.expression;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;

import com.google.common.collect.Iterators;

public class SchemaPath extends LogicalExpressionBase {

  private final NameSegment rootSegment;


  public static SchemaPath getSimplePath(String name){
    return getCompoundPath(name);
  }

  public static SchemaPath getCompoundPath(String... strings){
    List<String> paths = Arrays.asList(strings);
    Collections.reverse(paths);
    NameSegment s = null;
    for(String p : paths){
      s = new NameSegment(p);
    }
    return new SchemaPath(s);
  }



  /**
   *
   * @param simpleName
   */
  @Deprecated
  public SchemaPath(String simpleName, ExpressionPosition pos){
    super(pos);
    this.rootSegment = new NameSegment(simpleName);
    if(simpleName.contains("\\.")) throw new IllegalStateException("This is deprecated and only supports simpe paths.");
  }

  public SchemaPath(SchemaPath path){
    super(path.getPosition());
    this.rootSegment = path.rootSegment;
  }

  public SchemaPath(NameSegment rootSegment){
    super(ExpressionPosition.UNKNOWN);
    this.rootSegment = rootSegment;
  }

  public SchemaPath(NameSegment rootSegment, ExpressionPosition pos){
    super(pos);
    this.rootSegment = rootSegment;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
    return visitor.visitSchemaPath(this, value);
  }

  public SchemaPath getChild(String childPath){
    rootSegment.cloneWithNewChild(new NameSegment(childPath));
    return new SchemaPath(rootSegment);
  }

  public NameSegment getRootSegment() {
    return rootSegment;
  }

  @Override
  public MajorType getMajorType() {
    return Types.LATE_BIND_TYPE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((rootSegment == null) ? 0 : rootSegment.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if ( !(obj instanceof SchemaPath))
      return false;
    SchemaPath other = (SchemaPath) obj;
    if (rootSegment == null) {
      if (other.rootSegment != null)
        return false;
    } else if (!rootSegment.equals(other.rootSegment))
      return false;
    return true;
  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return Iterators.emptyIterator();
  }


  @Override
  public String toString() {
    String expr = ExpressionStringBuilder.toString(this);
    return "SchemaPath ["+ expr + "]";
  }

  public String toExpr(){
    return ExpressionStringBuilder.toString(this);
  }

}