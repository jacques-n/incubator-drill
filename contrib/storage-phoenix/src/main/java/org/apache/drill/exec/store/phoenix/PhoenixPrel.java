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
package org.apache.drill.exec.store.phoenix;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.visitor.PrelVisitor;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.store.phoenix.PhoenixGroupScan.PhoenixScans;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixRel.ImplementorContext;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementorImpl;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.RuntimeContextImpl;
import com.google.common.base.Preconditions;

/**
 * Represents a JDBC Plan once the children nodes have been rewritten into SQL.
 */
public class PhoenixPrel extends AbstractRelNode implements Prel {
  private static final String COLUMN_INFO_ATTR = "columnInfo";

  private final String hbaseTableName;
  private final double rows;
  private final PhoenixTableScan tableScan;

  private class SubsetRemover extends RelShuttleImpl {

    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof RelSubset) {
        return ((RelSubset) other).getBest().accept(this);
      } else {
        return super.visit(other);
      }
    }

  }

  public PhoenixPrel(RelOptCluster cluster, RelTraitSet traitSet, PhoenixIntermediatePrel prel) {
    super(cluster, traitSet);

    final RelNode input = prel.getInput().accept(new SubsetRemover());
    rows = input.getRows();
    tableScan = (PhoenixTableScan) input;
    hbaseTableName = tableScan.getTable().unwrap(PhoenixTable.class).pTable.getName().getString();
    rowType = prel.getRowType();

  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final PhoenixRel.Implementor phoenixImplementor = new PhoenixRelImplementorImpl(new RuntimeContextImpl());
    phoenixImplementor.pushContext(new ImplementorContext(false, true, ImmutableIntList.identity(tableScan.getRowType()
        .getFieldCount())));
    final QueryPlan plan = phoenixImplementor.visitInput(0, tableScan);
    phoenixImplementor.popContext();
    
    serializeColumnInfoIntoScan(plan.getContext().getScan(), tableScan.getRowType());

    final String storagePluginName = tableScan.getTable().getQualifiedName().iterator().next();

    try {
      PhoenixStoragePlugin plugin = (PhoenixStoragePlugin) creator.getContext().getStorage()
          .getPlugin(storagePluginName);

      // generate the scans.
      plan.iterator();

      List<List<Scan>> scans = plan.getScans();
      Preconditions.checkArgument(scans.size() == 1);
      return new PhoenixGroupScan(new PhoenixScans(scans.get(0)), plugin, rows, hbaseTableName);
    } catch (ExecutionSetupException | SQLException e) {
      throw new IOException(String.format("Failure while retrieving storage plugin %s", storagePluginName), e);
    }
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("table", hbaseTableName);
  }

  @Override
  public double getRows() {
    return rows;
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }
  
  protected static void serializeColumnInfoIntoScan(Scan scan, RelDataType rowType) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      DataOutputStream output = new DataOutputStream(stream);
      int count = rowType.getFieldCount();
      WritableUtils.writeVInt(output, count);
      for (int i = 0; i < count; i++) {
        WritableUtils.writeString(output, rowType.getFieldList().get(i).getName());
      }
      scan.setAttribute(COLUMN_INFO_ATTR, stream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        stream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }      
  }  
  
  protected static String[] deserializeColumnInfoFromScan(Scan scan) {
    byte[] info = scan.getAttribute(COLUMN_INFO_ATTR);
    if (info == null) {
      return null;
    }
    ByteArrayInputStream stream = new ByteArrayInputStream(info);
    try {
      DataInputStream input = new DataInputStream(stream);
      int count = WritableUtils.readVInt(input);
      String[] columnNames = new String[count];
      for (int i = 0; i < count; i++) {
        columnNames[i] = WritableUtils.readString(input);
      }
      return columnNames;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        stream.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
