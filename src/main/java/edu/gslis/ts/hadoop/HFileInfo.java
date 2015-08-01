package edu.gslis.ts.hadoop;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HFileInfo extends TSBase implements Tool 
{
    public int run(String[] args) throws Exception
    {
        String table = args[0];
        String inputPath = args[1];

        Configuration conf = HBaseConfiguration.create(getConf());

        Connection con = ConnectionFactory.createConnection(conf);
                
        RegionLocator locator = con.getRegionLocator(TableName.valueOf(table));
        Pair<byte[][], byte[][]> startEndKeys = locator.getStartEndKeys();

        FileSystem fs = FileSystem.get(conf);
        HFile.Reader hfr = HFile.createReader(fs, new Path(inputPath), new CacheConfig(getConf()), conf);
        hfr.loadFileInfo();
        
        byte[] first = hfr.getFirstRowKey();
        byte[] last =  hfr.getLastRowKey();

        System.out.println("first=" + Bytes.toStringBinary(first) +
                " last="  + Bytes.toStringBinary(last));
       
        for (int i=0; i < startEndKeys.getFirst().length; i++) {
            System.out.println(
                      Bytes.toStringBinary(startEndKeys.getFirst()[i]) + "," +
                      Bytes.toStringBinary(startEndKeys.getSecond()[i]) );            
        }
       

        int idx = Arrays.binarySearch(startEndKeys.getFirst(), first,
            Bytes.BYTES_COMPARATOR);
        System.out.println("Index=" + idx);
        if (idx < 0) {
            // not on boundary, returns -(insertion index).  Calculate region it
            // would be in.
            idx = -(idx + 1) - 1;
          }
        System.out.println("Index=" + idx);
        boolean lastKeyInRange =
                Bytes.compareTo(last, startEndKeys.getSecond()[idx]) < 0 ||
                Bytes.equals(startEndKeys.getSecond()[idx], HConstants.EMPTY_BYTE_ARRAY);
        
        System.out.println("lastKeyInRange=" + lastKeyInRange);
        return 0;        
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HFileInfo(), args);
        System.exit(res);
    }
}
