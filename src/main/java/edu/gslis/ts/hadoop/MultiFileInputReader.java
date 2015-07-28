package edu.gslis.ts.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import edu.gslis.streamcorpus.StreamItemWritable;

public class MultiFileInputReader extends
        CombineFileInputFormat<Text, StreamItemWritable> 
{
    public MultiFileInputReader() 
    {
        super();
        setMaxSplitSize(134217728); // 64 MB, default block size on hadoop
    }

    public RecordReader<Text, StreamItemWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException 
    {
        return new CombineFileRecordReader<Text, StreamItemWritable>(
                (CombineFileSplit) split, context,
                MultiFileThriftRecordReader.class);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) 
    {
        return false;
    }
}
