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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@JsonTypeName("jdbc-scan")
public class PhoenixGroupScan extends AbstractGroupScan {

  private final PhoenixScans scans;
  private final PhoenixStoragePlugin plugin;
  private final double rows;
  private final String table;

  // ephemeral
  private PhoenixScans[] scanAssignments;

  @JsonCreator
  public PhoenixGroupScan(
      @JsonProperty("scans") PhoenixScans scans,
      @JsonProperty("config") StoragePluginConfig config,
      @JsonProperty("rows") double rows,
      @JsonProperty("table") String table,
      @JacksonInject StoragePluginRegistry plugins) throws ExecutionSetupException {
    super("");
    this.scans = scans;
    this.plugin = (PhoenixStoragePlugin) plugins.getPlugin(config);
    this.rows = rows;
    this.table = table;
  }

  PhoenixGroupScan(PhoenixScans scans, PhoenixStoragePlugin plugin, double rows, String table) {
    super("");
    this.scans = scans;
    this.plugin = plugin;
    this.rows = rows;
    this.table = table;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    final int sliceCount = endpoints.size();
    assert scans.size() >= sliceCount;

    scanAssignments = new PhoenixScans[sliceCount];

    for (int i = 0; i < scanAssignments.length; i++) {
      scanAssignments[i] = new PhoenixScans();
    }

    int scanNumber = 0;
    for (Scan s : scans) {
      scanAssignments[scanNumber % sliceCount].add(s);
      scanNumber++;
    }

  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    return new PhoenixSubScan(scanAssignments[minorFragmentId], table, plugin);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return scans.size();
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(
        GroupScanProperty.NO_EXACT_ROW_COUNT,
        (long) Math.max(rows, 1),
        1,
        1);
  }

  public PhoenixScans getScans() {
    return scans;
  }

  @Override
  public String getDigest() {
    return scans.hashCode() + String.valueOf(plugin.getConfig());
  }

  public StoragePluginConfig getConfig() {
    return plugin.getConfig();
  }

  public String getTable() {
    return table;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    return new PhoenixGroupScan(scans, plugin, rows, table);
  }

  @JsonSerialize(using = PhoenixScans.Se.class)
  @JsonDeserialize(using = PhoenixScans.De.class)
  public static class PhoenixScans extends ArrayList<Scan> {

    public PhoenixScans(Collection<? extends Scan> c) {
      super(c);
    }

    public PhoenixScans() {
      super();
    }


    public static class De extends StdDeserializer<PhoenixScans>
    {
      public De() {
        super(PhoenixScans.class);
      }

      public PhoenixScans deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException, JsonProcessingException {

        // move past array start
        jp.nextToken();

        PhoenixScans scans = new PhoenixScans();
        while (jp.nextToken() != JsonToken.END_ARRAY) {
          scans.add(ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(jp.getBinaryValue())));
        }
        return scans;
      }
    }

    public static class Se extends StdSerializer<PhoenixScans>
    {
      public Se()
      {
        super(PhoenixScans.class);
      }

      public void serialize(PhoenixScans value, JsonGenerator jgen, SerializerProvider provider)
          throws IOException, JsonGenerationException
      {
        jgen.writeStartArray();

        for (Scan s : value) {
          jgen.writeBinary(ProtobufUtil.toScan(s).toByteArray());
        }
        jgen.writeEndArray();

      }
    }
  }


}