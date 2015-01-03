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
package org.apache.drill.exec.planner.sql.parser;

import java.util.Map;

import org.apache.drill.exec.util.JsonStringHashMap;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlWriter;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql2rel.SqlRexContext;
import org.eigenbase.sql2rel.SqlRexConvertable;

public class JsonLiteral extends SqlLiteral implements SqlRexConvertable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JsonLiteral.class);

  public JsonLiteral(Map<String, Object> value, SqlParserPos pos) {
    super(value, SqlTypeName.MAP, pos);
  }

  public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
    RelDataType type = typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR, 2048), typeFactory.createSqlType(SqlTypeName.ANY));
    return type;
  }

  @Override
  public RexNode convertToRex(SqlRexContext ctxt) {
    return makeMapLiteral(ctxt.getRexBuilder(), (Comparable) this.value);
  }

  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    super.validate(validator, scope);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
  }

  private class LiteralExposer extends RexBuilder {
    public LiteralExposer(RexBuilder builder) {
      super(builder.getTypeFactory());
    }

    public RexLiteral makeMapLiteral(Comparable obj){
      return makeLiteral(obj, createSqlType(getTypeFactory()), SqlTypeName.MAP);
    }

  }

  private RexLiteral makeMapLiteral(RexBuilder builder, Comparable obj){
    LiteralExposer e = new LiteralExposer(builder);
    return e.makeMapLiteral(obj);
  }

  public static Map<String, Object> createNewMap(){
    return new JsonStringHashMap();
  }
}
