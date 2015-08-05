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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;


/**
 * Non-splitable CombineFileInputFormat. Modify setMaxSplitSize to control
 * the number of mappers used on the input directory.
 */
public class ThriftFileInputFormat extends
    CombineFileInputFormat<Text, StreamItemWritable> 
{

    public ThriftFileInputFormat() 
    {
        super();
//        setMaxSplitSize(1073741824); // 1024 MB
//        setMaxSplitSize(268435456); // 256 MB
//        setMaxSplitSize(536870912); // 512 MB,
        setMaxSplitSize(134217728); // 128 MB, default block size on hadoop
    }

    @Override
    public RecordReader<Text, StreamItemWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<Text, StreamItemWritable>((CombineFileSplit)split, context, 
                ThriftRecordReader.class);
    }
}