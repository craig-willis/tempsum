package edu.gslis.ts.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.gslis.streamcorpus.StreamItemWritable;
import edu.gslis.streamcorpus.ThriftFileInputFormat;

/**
 * Process the streamcorpus. Score each streamitem with respect to the queries.
 * Filter streamcorpus, creating an Hbase entry for the streamitem for the
 * top-scoring query.
 */
public class ThriftWordCount extends TSBase implements Tool {

    public static class ThriftWordCountMapper extends
            Mapper<Text, StreamItemWritable, Text, IntWritable> 
    {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Text key, StreamItemWritable item, Context context)
                throws IOException, InterruptedException 
        {

            if (item.getBody() == null)
                return;

            String docText = item.getBody().getClean_visible();
            
            StringTokenizer itr = new StringTokenizer(docText);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    
    
    public static class ThriftWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

        IntWritable sum = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int i = 0;
                for (IntWritable val : values) {
                    i += val.get();
                }

                sum.set(i);
                context.write(key, sum);
        }
     }

    public int run(String[] args) throws Exception
    {
        String inputPath = args[0];
        Path outputPath = new Path(args[1]);

        Configuration config = HBaseConfiguration.create(getConf());
        Job job = Job.getInstance(config);
        job.setJarByClass(ThriftWordCount.class);
        job.setInputFormatClass(ThriftFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(ThriftWordCountReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outputPath);     
        job.setMapperClass(ThriftWordCountMapper.class);

        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job");
        }

        return 0;        
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ThriftWordCount(), args);
        System.exit(res);
    }
}
