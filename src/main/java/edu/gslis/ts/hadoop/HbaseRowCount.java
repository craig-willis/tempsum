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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Read an HBase table containing serialized thrift entries, order by timestamp.
 */
public class HbaseRowCount extends TSBase implements Tool {


    public static class HbaseCountTableMapper extends TableMapper<Text, IntWritable> 
    {
               
        private final static IntWritable one = new IntWritable(1);
        private Text key = new Text("all");
        
        public void map(ImmutableBytesWritable row, Result value, Context context) 
                throws InterruptedException, IOException 
        {
            
            context.write(key, one);
        }
    }
    

    public static class HbaseCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        IntWritable sum = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
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
        String tableName = args[0];
        Path outputPath = new Path(args[1]);
        
        Configuration config = HBaseConfiguration.create(getConf());
        Job job = Job.getInstance(config);
        job.setJarByClass(HbaseRowCount.class);
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan, 
                HbaseCountTableMapper.class, 
                Text.class, // mapper output key
                IntWritable.class, // mapper output value
                job
        );
        
        job.setReducerClass(HbaseCountReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileOutputFormat.setOutputPath(job, outputPath);

        
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HbaseRowCount(),
                args);
        System.exit(res);
    }
}
