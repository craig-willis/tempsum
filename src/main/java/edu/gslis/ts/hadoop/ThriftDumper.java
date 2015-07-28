package edu.gslis.ts.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.gslis.streamcorpus.StreamItemWritable;
import edu.gslis.streamcorpus.ThriftFileInputFormat;
import edu.gslis.textrepresentation.FeatureVector;

/**
 * Dump the streamid and query id.
 */
public class ThriftDumper extends TSBase implements Tool {
    private static final Logger logger = Logger.getLogger(ThriftDumper.class);

    public static class ThriftDumperMapper extends
            Mapper<Text, StreamItemWritable, Text, IntWritable> 
    {
        double MU = 2500;
        
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();
        Map<String, Double> vocab = new TreeMap<String, Double>();

        Text streamId = new Text();
        IntWritable queryId = new IntWritable();
        String docText; 
        FeatureVector dv;
        double maxScore;
        int qid;
        String query;
        FeatureVector qv;
        String dateTime;
        
        
        public void map(Text key, StreamItemWritable item, Context context)
                throws IOException, InterruptedException 
        {
            //System.out.println(item.getStream_id());

            if (item.getBody() == null)
                return;
            
            
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
               
        
        protected void setup(Context context) {
            logger.info("Setup");
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
                    logger.error("Can't load cache files. Trying local cache");
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
                logger.error(ioe);
            }
        }

    }    

    public static class ThriftDumperReducer extends Reducer<Text, IntWritable, Text, IntWritable>  
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

    // http://www.ibm.com/developerworks/library/ba-mapreduce-biginsights-analysis/
    // http://www.ibm.com/developerworks/library/bd-hadoopcombine/
    public int run(String[] args) throws Exception
    {
        String inputPath = args[0];
        String outputPath = args[1];
        Path topicsFile = new Path(args[2]);
        Path vocabFile = new Path(args[3]);

        Configuration config = getConf();
        Job job = Job.getInstance(config);
        job.setJarByClass(ThriftDumper.class);
        job.setInputFormatClass(ThriftFileInputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(ThriftDumperReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));     
        job.addCacheFile(topicsFile.toUri());
        job.addCacheFile(vocabFile.toUri());
        
        job.setMapperClass(ThriftDumperMapper.class);

        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job");
        }

        return 0;        
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ThriftDumper(), args);
        System.exit(res);
    }
}
