package edu.gslis.ts;
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.bag.HashBag;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.tukaani.xz.XZInputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import streamcorpus_v3.StreamItem;
import edu.gslis.textrepresentation.FeatureVector;
import edu.gslis.utils.Stopper;
import gov.loc.repository.pairtree.Pairtree;


/**
 * Simple 
 */
public class RunQuery 
{
    final static double MU = 2500;

	public static void main(String[] args) 
	{
    	try
    	{
    		// Get the commandline options
    		Options options = createOptions();
    		CommandLineParser parser = new GnuParser();
    		CommandLine cmd = parser.parse( options, args);
        	
        	String inputPath = cmd.getOptionValue("input");
            String eventsPath = cmd.getOptionValue("events");
            String stopPath = cmd.getOptionValue("stop");
            int queryId = Integer.valueOf(cmd.getOptionValue("query"));
            
            List<String> ids = FileUtils.readLines(new File(inputPath + File.separator + "ids.txt"));
            
        	Stopper stopper = new Stopper(stopPath);
            Map<Integer, FeatureVector> queries = readEvents(eventsPath, stopper);
            
            FeatureVector query = queries.get(queryId);
            
            Pairtree ptree = new Pairtree();
            Bag<String> words = new HashBag<String>();

            for (String streamId: ids) 
            {
                
                String ppath = ptree.mapToPPath(streamId.replace("-", ""));
                                
                String inpath = inputPath + File.separator + ppath + File.separator + streamId + ".xz";
//                System.out.println(inpath);
                File infile = new File(inpath);
                InputStream in = new XZInputStream(new FileInputStream(infile));
                
                TTransport inTransport = 
                    new TIOStreamTransport(new BufferedInputStream(in));
                TBinaryProtocol inProtocol = new TBinaryProtocol(inTransport);
                inTransport.open();
                final StreamItem item = new StreamItem();

                while (true)
                {
                    try {
                        item.read(inProtocol);
//                        System.out.println("Read " + item.stream_id);

                    } catch (TTransportException tte) {
                        // END_OF_FILE is used to indicate EOF and is not an exception.
                        if (tte.getType() != TTransportException.END_OF_FILE) 
                            tte.printStackTrace();
                        break;
                    }
                }
                
                // Do something with this document...
                String docText = item.getBody().getClean_visible();

                StringTokenizer itr = new StringTokenizer(docText);
                while (itr.hasMoreTokens()) {
                    words.add(itr.nextToken());
                }
                
                inTransport.close();
                
            }
            
            for (String term: words.uniqueSet()) {
                System.out.println(term + ":" + words.getCount(term));
            }
	    
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
	}

	public static Options createOptions()
	{
		Options options = new Options();
		options.addOption("input", true, "Input chunk file");
        options.addOption("events", true, "Events file");
        options.addOption("stop", true, "Stoplist");
        options.addOption("query", true, "Query");
		return options;
	}
	
    public static Map<Integer, FeatureVector> readEvents(String path, Stopper stopper) 
    {
        Map<Integer, FeatureVector> queries = new TreeMap<Integer, FeatureVector>();

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory
                    .newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            

            Document doc = db.parse(new File(path));
            
            NodeList events = doc.getDocumentElement().getElementsByTagName("event");
            for (int i = 0; i < events.getLength(); i++) {
                Node event = events.item(i);
                NodeList elements = event.getChildNodes();
                int id = -1;
                String query = "";

                for (int j = 0; j < elements.getLength(); j++) {
                    Node element = elements.item(j);
                    if (element == null) continue;
                    
                    if (element.getNodeName().equals("id"))
                        id = Integer.parseInt(element.getTextContent());
                    else if (element.getNodeName().equals("query"))
                        query = element.getTextContent();
                }               
                queries.put(id, new FeatureVector(query, stopper));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }
}
