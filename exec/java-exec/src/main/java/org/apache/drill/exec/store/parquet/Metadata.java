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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetFileWriter;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Metadata {

  private static Configuration conf = new Configuration();
  private static FileSystem fs;
  private static final String PATH = "/drill/tpchmulti/lineitem/";

  public static void main(String[] args) throws IOException {
//    createMeta(PATH);
    createBlockMeta(PATH);
  }

  public static void createMeta(String path) throws IOException {
    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    FileStatus fileStatus = fs.getFileStatus(new Path(path));
    List<Footer> footers = ParquetFileReader.readAllFootersInParallel(conf, fileStatus);

    ParquetFileWriter.writeMetadataFile(conf, new Path(path), footers);
  }

  public static void createBlockMeta(String path) throws IOException {
    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    assert fileStatus.isDirectory() : "Must select a directory";
    List<FileBlockLocations> fileBlockLocationsList = Lists.newArrayList();
    for (FileStatus file : fs.listStatus(p)) {
      BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
      fileBlockLocationsList.add(new FileBlockLocations(file.getPath().toString(), Arrays.asList(blockLocations)));
    }

    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    FileOutputStream os = new FileOutputStream("/tmp/blocks");
//    for (FileBlockLocations fileBlockLocations : fileBlockLocationsList) {
//      mapper.writeValue(os, fileBlockLocations);
//    }
    mapper.writeValue(os, fileBlockLocationsList);
    os.flush();
    os.close();

    FileInputStream is = new FileInputStream("/tmp/blocks");
    List<FileBlockLocations> locations = mapper.readValue(is, List.class);
    System.out.println(locations);
  }

  public static class FileBlockLocations {
    @JsonProperty
    public String path;
    @JsonProperty
    public List<BlockLocation> blockLocations;

    public FileBlockLocations() {
     super();
    }

    public FileBlockLocations(String path, List<BlockLocation> blockLocations) {
      this.path = path;
      this.blockLocations = blockLocations;
    }

    @Override
    public String toString() {
      return String.format("path: %s blocks: %s", path, blockLocations);
    }
  }
}
