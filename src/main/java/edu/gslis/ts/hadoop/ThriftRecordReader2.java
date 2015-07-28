package edu.gslis.ts.hadoop;
/*******************************************************************************
 * Copyright 2012 Edgar Meij
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.tukaani.xz.XZInputStream;

import streamcorpus_v3.StreamItem;
import edu.gslis.streamcorpus.StreamItemWritable;

/**
 * RecordReader that emits filename, StreamItemWritable pairs. 
 * 
 * @author emeij
 *
 */
public class ThriftRecordReader2 extends RecordReader<Text, StreamItemWritable> {

  private InputStream in;
  private TTransport inTransport;
  private TBinaryProtocol inProtocol;
  private long start;
  private long length;
  private long position;
  private Text key = new Text();
  private StreamItemWritable value = new StreamItemWritable();
  private FileSplit fileSplit;
  private Configuration conf;

  public ThriftRecordReader2(FileSplit fileSplit, Configuration conf)
      throws IOException {
    this.fileSplit = fileSplit;
    this.conf = conf;
  }

  @Override
  public void close() throws IOException {
    if (in != null)
      in.close();
    if (inTransport != null)
      inTransport.close();
  }

  /** Returns our progress within the split, as a float between 0 and 1. */
  @Override
  public float getProgress() {

    if (length == 0)
      return 0.0f;

    return Math.min(1.0f, position / (float) length);

  }

  /** Boilerplate initialization code for file input streams. */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    conf = context.getConfiguration();
    fileSplit = (FileSplit) split;
    start = fileSplit.getStart();
    length = fileSplit.getLength();
    position = 0;

    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);

    in = fs.open(path); 

    if (path.toUri().toString().endsWith("xz"))   
        in = new XZInputStream(in);

    try
    {
        inTransport = 
                new TIOStreamTransport(new BufferedInputStream(in));
        inProtocol = new TBinaryProtocol(inTransport);
        inTransport.open();
    } catch (Exception e) {
        throw new IOException(e);
    }
  }

  @Override
  /**
   * parse the next key value, update position and return true
   */
  public boolean nextKeyValue() throws IOException, InterruptedException {

    key.set(fileSplit.getPath().toString());

    try
    {
        value.read(inProtocol);
        position = length - in.available() - start;
    } catch (TTransportException te) {
        if (te.getType() == TTransportException.END_OF_FILE) {
            return false;
        } else {
            System.out.println("Error " + te.getMessage() + ": " + fileSplit.getPath().toString());
            return false;
//            throw new IOException(fileSplit.getPath().toString(), te);
        }
    } catch (TException e) {        
        System.out.println("Error " + e.getMessage() + ": " + fileSplit.getPath().toString());
        return false;
    }

    return true;

  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public StreamItemWritable getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }
}
