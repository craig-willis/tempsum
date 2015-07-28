package edu.gslis.ts.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import edu.gslis.streamcorpus.StreamItemWritable;
import edu.gslis.streamcorpus.ThriftFileInputFormat;
import edu.gslis.textrepresentation.FeatureVector;

/**
 * Dump the streamid and query id.
 */
public class ThriftFilter extends TSBase implements Tool {
    private static final Logger logger = Logger.getLogger(ThriftFilter.class);

    public static class ThriftFilterMapper extends
            Mapper<Text, StreamItemWritable, ImmutableBytesWritable, Put> 
    {
        double MU = 2500;
        
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();
        Map<String, Double> vocab = new TreeMap<String, Double>();
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        ImmutableBytesWritable hbaseTable = new ImmutableBytesWritable();

        public void map(Text key, StreamItemWritable item, Context context)
                throws IOException, InterruptedException 
        {

            if (item.getBody() == null)
                return;
            
            
            String streamid = item.getStream_id();

            String docText = item.getBody().getClean_visible();
            if (docText != null && docText.length() > 0) 
                {
                FeatureVector dv = new FeatureVector(docText, null);
    
                double maxScore = Double.NEGATIVE_INFINITY;
                int queryId = -1;
                for (int id : queries.keySet()) 
                {
                    FeatureVector qv = queries.get(id);
                    double score = kl(dv, qv, vocab, MU);
                    if (score > maxScore) {
                        queryId = id;
                        maxScore = score;
                    }
                }
                
                String dateTime = "";
                long epoch = 0;
                if (item.stream_time != null && item.stream_time.zulu_timestamp != null)
                {
                    dateTime = item.stream_time.zulu_timestamp;
                    DateTimeFormatter dtf = ISODateTimeFormat.dateTime();
                    epoch = dtf.parseMillis(dateTime);
                }
    
                try {
                    Put put = new Put(Bytes.toBytes(streamid));
                    put.add(Bytes.toBytes("md"), Bytes.toBytes("query"), Bytes.toBytes(queryId));
                    put.add(Bytes.toBytes("md"), Bytes.toBytes("epoch"), Bytes.toBytes(epoch));
                    //put.add(Bytes.toBytes("si"), Bytes.toBytes("streamitem"), Bytes.toBytes(cleanVisible));
                    put.add(Bytes.toBytes("si"), Bytes.toBytes("streamitem"), Bytes.toBytes(streamid));
//                    put.add(Bytes.toBytes("si"), Bytes.toBytes("streamitem"), serializer.serialize(item));
                    context.write(hbaseTable, put);  
                } catch (Exception e) {
                    e.printStackTrace();
                }
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

    public int run(String[] args) throws Exception
    {
        String tableName = args[0];
        String inputPath = args[1];
        Path topicsFile = new Path(args[2]);
        Path vocabFile = new Path(args[3]);
        //String tmpDir = args[4];

        Configuration config = HBaseConfiguration.create(getConf());
        //config.set("hadoop.tmp.dir", tmpDir);
        Job job = Job.getInstance(config);
        job.setJarByClass(ThriftFilter.class);
        job.setInputFormatClass(ThriftFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Writable.class);

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
        job.setNumReduceTasks(0); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Put.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
        
        job.addCacheFile(topicsFile.toUri());
        job.addCacheFile(vocabFile.toUri());
        
        job.setMapperClass(ThriftFilterMapper.class);

        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job");
        }

        return 0;        
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ThriftFilter(), args);
        System.exit(res);
    }
}
