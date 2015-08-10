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

package edu.gslis.ts.hadoop;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
 * Process the streamcorpus. Score each streamitem with respect to the input 
 * queries. Create an HBase entry for each serialized streamitem using the 
 * top-scoring query.
 */
public class ThriftBulkLoader extends TSBase implements Tool 
{
    private static final Logger logger = Logger.getLogger(ThriftBulkLoader.class);

    
    public static class ThriftFilterMapper extends
            Mapper<Text, StreamItemWritable, ImmutableBytesWritable, Put> 
    {
        double MU = 2500;
        
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();
        Map<String, Double> vocab = new TreeMap<String, Double>();
        Map<String, Integer> dateBins = new HashMap<String, Integer>();
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        ImmutableBytesWritable hbaseTable = new ImmutableBytesWritable();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH");
        Put put;
        DateTimeFormatter dtf = ISODateTimeFormat.dateTime();
        String streamid;
        String docText;
        FeatureVector dv;
        double maxScore;
        int queryId;
        String query;
        FeatureVector qv;
        double score;
        String dateTime;
        long epoch;
        String cleanVisible;
        String dateStr;
        String bin;
        String queryStr;
        public void map(Text key, StreamItemWritable item, Context context)
                throws IOException, InterruptedException 
        {

            if (item == null || item.getBody() == null)
                return;
            
            streamid = item.getStream_id();

//            System.out.println("Processing item " + streamid);

            docText = item.getBody().getClean_visible();
            if (docText != null && docText.length() > 0) 
            {
                dv = new FeatureVector(docText, null);
    
                maxScore = Double.NEGATIVE_INFINITY;
                queryId = -1;
                for (int id : queries.keySet()) 
                {
                    qv = queries.get(id);
                    score = kl(dv, qv, vocab, MU);
                    if (score > maxScore) {
                        queryId = id;
                        maxScore = score;
                    }
                }
                
                dateTime = "";
                epoch = 0;
                if (item.stream_time != null && item.stream_time.zulu_timestamp != null)
                {
                    dateTime = item.stream_time.zulu_timestamp;
                    epoch = dtf.parseMillis(dateTime);
                    dateStr = df.format(epoch);
                    bin = String.format("%04d", dateBins.get(dateStr));
                }
    
                queryStr = String.format("%02d", queryId);
                
                String rowid = queryStr + bin + "." + streamid;
//                String rowid = DigestUtils.md5Hex(queryStr + bin + "." + streamid);                

                try {
                    byte[] serialized = serializer.serialize(item);
                    if (serialized.length < 64*1024*1024) {
                        put = new Put(Bytes.toBytes(rowid));
                        put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("query"), Bytes.toBytes(queryId));
                        put.addColumn(Bytes.toBytes("md"), Bytes.toBytes("epoch"), Bytes.toBytes(epoch));
                        put.addColumn(Bytes.toBytes("si"), Bytes.toBytes("streamitem"), serialized);
                        //put.addColumn(Bytes.toBytes("si"), Bytes.toBytes("streamitem"), Bytes.toBytes(streamid));
                        ImmutableBytesWritable hkey = new ImmutableBytesWritable(Bytes.toBytes(rowid));
                        context.write(hkey, put); 
                    } else {
                        System.out.println(streamid + " exceeds 64M limit, skipping");
                    }
                        
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
               
        
        protected void setup(Context context)         
        {
            Configuration configuration = context.getConfiguration();       
            String tableName = configuration.get("hbase.table.name");
            hbaseTable = new ImmutableBytesWritable(Bytes.toBytes(tableName)); 
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
                        if (file.toString().contains("date"))
                            dateBins = readDateBins(file.toString(), fs);
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

    // http://databuzzprd.blogspot.com/2013/11/bulk-load-data-in-hbase-table.html#.VazwFSqrSgQ
    // http://blog.cloudera.com/blog/2013/09/how-to-use-hbase-bulk-loading-and-why/
    // http://www.deerwalk.com/blog/bulk-importing-data/
    //https://github.com/Paschalis/HBase-Bulk-Load-Example/blob/master/src/cy/ac/ucy/paschalis/hbase/bulkimport/Driver.java
    //https://www.rswebsols.com/tutorials/programming/bulk-load-big-data-hadoop-hbase-table
    
    public int run(String[] args) throws Exception
    {
        String tableName = args[0];
        String inputPath = args[1];
        String outputPath = args[2];
        Path topicsFile = new Path(args[3]);
        Path vocabFile = new Path(args[4]);
        Path dateBinFile = new Path(args[5]);

        Configuration config = getConf();
        config.set("hbase.table.name", tableName);
        HBaseConfiguration.addHbaseResources(config);
        
        Job job = Job.getInstance(config);     
        job.setJarByClass(ThriftBulkLoader.class);       
        job.setJobName("Bulk Loading HBase Table::"+ tableName);        
        job.setInputFormatClass(ThriftFileInputFormat.class);     
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);     
        job.setMapperClass(ThriftFilterMapper.class);      
        
        Path output = new Path(outputPath);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, output);        

        job.setMapOutputValueClass(Put.class); 
        
        job.addCacheFile(topicsFile.toUri());
        job.addCacheFile(vocabFile.toUri());
        job.addCacheFile(dateBinFile.toUri());
        
        job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
        job.getConfiguration().setClass("mapred.map.output.compression.codec",
            org.apache.hadoop.io.compress.SnappyCodec.class,
            org.apache.hadoop.io.compress.CompressionCodec.class);
        job.getConfiguration().set("hfile.compression",
            Compression.Algorithm.SNAPPY.getName());
        
        //RegionLocator regionLocator = conn.getRegionLocator(tableName);
        //HFileOutputFormat2.configureIncrementalLoad(job, new HTable(config,tableName));
        
        Connection con = ConnectionFactory.createConnection(config);
        TableName htableName = TableName.valueOf(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, con.getTable(htableName), con.getRegionLocator(htableName));
        
        job.waitForCompletion(true);        
        if (job.isSuccessful()) {
            // Couldn't find a better way to do this. The LoadIncrementalHFiles
            // seems to want 777 permissions on the output directory.
            try {
                Runtime rt = Runtime.getRuntime();
                rt.exec("hadoop fs -chmod -R 777 " + output);
            } catch (Exception e) {
                e.printStackTrace();
            }
            /*
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
            HTable htable = new HTable(config, tableName);
            loader.doBulkLoad(new Path(outputPath), htable);
            */

        } else {
            throw new IOException("error with job");
        }
        
        return 0; 
        
        // - 
        
        /*
        Job job = Job.getInstance(config);
        job.setJarByClass(ThriftBulkLoader.class);
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
        job.setMapOutputValueClass(Put.class);  
        job.setInputFormatClass(ThriftFileInputFormat.class);
        
        //HFileOutputFormat2.configureIncrementalLoad(job, htable);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));        
        
        job.addCacheFile(topicsFile.toUri());
        job.addCacheFile(vocabFile.toUri());
        
        job.setMapperClass(ThriftFilterMapper.class);
        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job");
        }
        
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(config);
        loader.doBulkLoad(new Path(outputPath), htable);

        return 0;        
        */
    }

    public static void main(String[] args) throws Exception {
                
        int res = ToolRunner.run(new Configuration(), new ThriftBulkLoader(), args);
        System.exit(res);
    }
}
