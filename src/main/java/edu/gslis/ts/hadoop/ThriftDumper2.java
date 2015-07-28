    package edu.gslis.ts.hadoop;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.tukaani.xz.XZInputStream;

import streamcorpus_v3.StreamItem;
import edu.gslis.textrepresentation.FeatureVector;

/**
 * Process a list of files, let the Mapper handle decompress and deserialization
 * Dump the streamid and query id.
 */
public class ThriftDumper2 extends TSBase implements Tool {

    public static class ThriftDumper2Mapper extends
            Mapper<LongWritable, Text, Text, IntWritable> 
    {

        double MU = 2500;
        
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();
        Map<String, Double> vocab = new TreeMap<String, Double>();
        //TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());


        Text streamId = new Text();
        IntWritable queryId = new IntWritable();
        String docText; 
        FeatureVector dv;
        double maxScore;
        int qid;
        String query;
        FeatureVector qv;
        String dateTime;
        
        Charset charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = charset.newDecoder();


        
        public void map(LongWritable key, Text path, Context context)
                throws IOException, InterruptedException 
        {
                        
            FileSystem fs = FileSystem.get(context.getConfiguration());
            
            InputStream is = fs.open(new Path(path.toString()));
            
            if (path.toString().endsWith("xz"))        
                is = new XZInputStream(is);
                            
            try
            {
                TTransport inTransport = 
                    new TIOStreamTransport(new BufferedInputStream(is));
                TBinaryProtocol inProtocol = new TBinaryProtocol(inTransport);
                inTransport.open();
                
                try
                {
        
                    // Run through items in the thrift file
                    while (true) 
                    {
                        final StreamItem item = new StreamItem();
                        item.read(inProtocol);
                        if (item.body == null || item.body.clean_visible == null)
                            continue;

                        docText = item.getBody().getClean_visible();
                        if (docText != null && docText.length() > 0) 
                        {
                            dv = new FeatureVector(docText, null);
                
                            maxScore = Double.NEGATIVE_INFINITY;
                            qid = -1;
                            for (int id : queries.keySet()) 
                            {
                                qv = queries.get(id);
                                double score = kl(dv, qv, vocab, MU);
                                if (score > maxScore) {
                                    qid = id;
                                    maxScore = score;
                                }
                            }                

                            streamId.set(item.getStream_id());
                            queryId.set(qid);
                
                            context.write(streamId, queryId);
                        }
                    }   
                } catch (TTransportException te) {
                    if (te.getType() == TTransportException.END_OF_FILE) {
                    } else {
                        throw te;
                    }
                }
                inTransport.close();
            } catch (Exception e) {
                System.out.println("Error processing " + path.toString());
                e.printStackTrace();
            }
            is.close();
        }
        
        
        protected void setup(Context context) {
            try {
                // Side-load the topics
                               
                URI[] files = context.getCacheFiles();
                                
                if (files != null) {
                    FileSystem fs = FileSystem.get(context.getConfiguration());

                    for (URI file : files) {
                        
                        if (file.toString().contains("topics"))
                            queries = readEvents(file.toString(), fs);
                        if (file.toString().contains("vocab"))
                            vocab = readVocab(file.toString(), fs);
                    }
                } else {
                    Path[] paths = context.getLocalCacheFiles();
                    for (Path path : paths) {
                        if (path.toString().contains("topics"))
                            queries = readEvents(path.toString(), null);
                        if (path.toString().contains("vocab"))
                            vocab = readVocab(path.toString(), null);
                    }
                }

            } catch (Exception ioe) {
                ioe.printStackTrace();
            }
        }
    }    

    public static class ThriftDumper2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>  
    {

        IntWritable sum = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            for (IntWritable val : values) 
            {
                context.write(key, val);
            }
        }
     }

    public int run(String[] args) throws Exception
    {
        String inputPath = args[0];
        String outputPath = args[1];
        Path topicsFile = new Path(args[2]);
        Path vocabFile = new Path(args[3]);

        Configuration config = getConf();
        Job job = Job.getInstance(config);
        job.setJarByClass(ThriftDumper2.class);
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setMapperClass(ThriftDumper2Mapper.class);
        job.setReducerClass(ThriftDumper2Reducer.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));     
        job.addCacheFile(topicsFile.toUri());
        job.addCacheFile(vocabFile.toUri());


        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job");
        }

        return 0;        
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ThriftDumper2(), args);
        System.exit(res);
    }
}
