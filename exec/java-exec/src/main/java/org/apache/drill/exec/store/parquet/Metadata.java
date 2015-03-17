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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetFileWriter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadata {

  public static void main(String[] args) throws IOException {

    String path = "/drill/tpchmulti/lineitem/";

    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(conf);
//    createMeta(conf, fs, path);
    Map<String,List<BlockLocation>> m = readBlockMeta(fs, path + "/.drill.blocks");
    for (String s : m.keySet()) {
      System.out.println(s);
    }
  }

  public static void createMeta(Configuration conf, FileSystem fs, String path) throws IOException {
    if (fs == null) {
      fs = FileSystem.get(conf);
    }
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    List<FileStatus> fileStatuses = getFileStatuses(fs, fileStatus);
    List<Footer> footers = ParquetFileReader.readAllFootersInParallel(conf, fileStatuses);

    ParquetFileWriter.writeMetadataFile(conf, p, footers);
    createBlockMeta(fs, fileStatuses, p);
  }

  private static List<FileStatus> getFileStatuses(FileSystem fs, FileStatus fileStatus) throws IOException {
    List<FileStatus> statuses = Lists.newArrayList();
    if (fileStatus.isDirectory()) {
      for (FileStatus child : fs.listStatus(fileStatus.getPath(), new OutputFilesFilter())) {
        statuses.addAll(getFileStatuses(fs, child));
      }
    } else {
      statuses.add(fileStatus);
    }
    return statuses;
  }

  public static void createBlockMeta(FileSystem fs, List<FileStatus> files, Path p) throws IOException {
    List<FileBlockLocations> fileBlockLocationsList = Lists.newArrayList();
    for (FileStatus file : files) {
      BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
      fileBlockLocationsList.add(new FileBlockLocations(Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString(), Arrays.asList(blockLocations)));
    }

    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    FSDataOutputStream os = fs.create(new Path(p, ".drill.blocks"));
    mapper.writeValue(os, fileBlockLocationsList);
    os.flush();
    os.close();
  }

  public static Map<String,List<BlockLocation>> readBlockMeta(FileSystem fs, String path) throws IOException {
    Path p = new Path(path);
    ObjectMapper mapper = new ObjectMapper();
    FSDataInputStream is = fs.open(p);
    List<FileBlockLocations> fileBlockLocationsList = mapper.readValue(is, new TypeReference<List<FileBlockLocations>>(){});
    Map<String,List<BlockLocation>> map = new HashMap();
    for (FileBlockLocations fileBlockLocations : fileBlockLocationsList) {
      map.put(fileBlockLocations.path, fileBlockLocations.blockLocations);
    }
    return map;
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
