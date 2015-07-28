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
 * Non-splitable CombineFileInputFormat.
 */
public class ThriftFileInputFormat extends
    CombineFileInputFormat<Text, StreamItemWritable> 
{

    public ThriftFileInputFormat() 
    {
        super();
        setMaxSplitSize(268435456); // 256 MB
//      setMaxSplitSize(536870912); // 512 MB, default block size on hadoop
//      setMaxSplitSize(134217728); // 128 MB, default block size on hadoop
    }

    @Override
    public RecordReader<Text, StreamItemWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<Text, StreamItemWritable>((CombineFileSplit)split, context, 
                ThriftRecordReader.class);
    }
}