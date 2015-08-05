/*******************************************************************************
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

package edu.gslis.streamcorpus;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;
import org.tukaani.xz.XZInputStream;


/**
 * Combinable record reader for thrift files. Since the thrift/chunk
 * files used in the KBA streamcorpus tend to be smaller than the 
 * default HDFS block size, we use the CombineFileSplot paradigm
 * to combine multiple compressed thrift files and reduce the number
 * of mappers required for job execution.
 * 
 * @author cwillis
 */
public class ThriftRecordReader extends
        RecordReader<Text, StreamItemWritable> 
{
    private long startOffset;
    private long end;
    private long pos;
    private FileSystem fs;
    private Path path;
    private Text key = new Text();
    private StreamItemWritable value = new StreamItemWritable();
    private TProtocol tp;
    private FSDataInputStream in;

    public ThriftRecordReader(CombineFileSplit split,
            TaskAttemptContext context, Integer index) throws IOException {
        this.path = split.getPath(index);
        fs = this.path.getFileSystem(context.getConfiguration());
        this.startOffset = split.getOffset(index);
        this.end = startOffset + split.getLength(index);
        this.pos = startOffset;

        in = fs.open(path);

        if (path.toUri().toString().endsWith("xz"))
            tp = new TBinaryProtocol.Factory()
                    .getProtocol(new TIOStreamTransport(new XZInputStream(in)));
        else
            tp = new TBinaryProtocol.Factory()
                    .getProtocol(new TIOStreamTransport(in));

    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        // Won't be called, use custom Constructor
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        if (startOffset == end) {
            return 0;
        }
        return Math
                .min(1.0f, (pos - startOffset) / (float) (end - startOffset));
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

    @Override
    public boolean nextKeyValue() throws IOException {
        key.set(path.getName() + "-" + pos);

        if (in.available() > 0) {

            try {
                value.read(tp);
                pos = end - in.available() - startOffset;

            } catch (TTransportException tte) {
                // END_OF_FILE is used to indicate EOF and is not an exception.
                if (tte.getType() != TTransportException.END_OF_FILE) 
                    tte.printStackTrace();
                return false;
            } catch (Exception e) {
                e.printStackTrace();
                // throw new IOException(e);
            }
        } else
            return false;

        return true;
    }
}