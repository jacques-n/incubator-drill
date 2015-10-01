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
package org.apache.drill.exec.store.phoenix.rel;

import java.util.Arrays;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class PhoenixServerSortRule extends ConverterRule {
  
  private static Predicate<Sort> IS_CONVERTIBLE = new Predicate<Sort>() {
      @Override
      public boolean apply(Sort input) {
          return PhoenixConverterRules.isConvertible(input);
      }            
  };
  
  private static Predicate<Sort> SORT_ONLY = new Predicate<Sort>() {
      @Override
      public boolean apply(Sort input) {
          return !input.getCollation().getFieldCollations().isEmpty()
                  && input.offset == null
                  && input.fetch == null;
      }            
  };
  
  public static final PhoenixServerSortRule CONVERTIBLE_SERVER = new PhoenixServerSortRule(Predicates.and(Arrays.asList(SORT_ONLY, IS_CONVERTIBLE)), PhoenixRel.SERVER_CONVENTION);

  private final Convention inputConvention;

  private PhoenixServerSortRule(Predicate<Sort> predicate, Convention inputConvention) {
    super(Sort.class, 
        predicate, 
        Convention.NONE, PhoenixRel.SERVER_CONVENTION, "PhoenixServerSortRule:" + inputConvention.getName());
    this.inputConvention = inputConvention;
  }

  public RelNode convert(RelNode rel) {
    final Sort sort = (Sort) rel;
    return PhoenixServerSort.create(
        convert(
            sort.getInput(), 
            sort.getInput().getTraitSet().replace(inputConvention)),
            sort.getCollation());
  }

}
