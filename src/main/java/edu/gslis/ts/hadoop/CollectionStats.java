package edu.gslis.ts.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.gslis.streamcorpus.StreamItemWritable;
import edu.gslis.streamcorpus.ThriftFileInputFormat;
import edu.gslis.textrepresentation.FeatureVector;

/**
 * Process the streamcorpus. Calculate overall term frequencies
 */
public class CollectionStats extends Configured implements Tool 
{
    
    public static class ThriftFilterMapper extends
            Mapper<Text, StreamItemWritable, Text, DoubleWritable> 
    {
        Text term = new Text();
        DoubleWritable weight = new DoubleWritable();

        public void map(Text key, StreamItemWritable value, Context context)
                throws IOException, InterruptedException 
        {
            String text = value.getBody().getClean_visible();
            FeatureVector dv = new FeatureVector(text, null);
            for (String feature: dv.getFeatures()) {
                term.set(feature);
                weight.set(dv.getFeatureWeight(feature));
                context.write(term, weight);
            }
        }
    }

    public static class ThriftFilterReducer extends
            TableReducer<Text, DoubleWritable, ImmutableBytesWritable> 
    {
        
        public void reduce(Text text, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException 
        {
            double sum = 0;
            for (DoubleWritable weight: values) {
                sum += weight.get();
            }
            
            Put put = new Put(Bytes.toBytes(text.toString()));
            put.add(Bytes.toBytes("cf"), Bytes.toBytes("weight"), Bytes.toBytes(sum));

            context.write(null, put);

        }
    }

    public int run(String[] args) throws Exception {
        String tableName = args[0];
        String inputPath = args[1];

        Configuration config = HBaseConfiguration.create(getConf());
        Job job = Job.getInstance(config);
        job.setJarByClass(CollectionStats.class);
        job.setInputFormatClass(ThriftFileInputFormat.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableReducerJob(tableName,
                ThriftFilterReducer.class, job);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(ThriftFilterMapper.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CollectionStats(), args);
        System.exit(res);
    }
}
