//Used for insertion into HBase

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class JSONParsing {
    public static void main(String [] args) throws Exception {
    	
    	
    	
    	//JSON parse test table
    	 // Instantiating configuration class
        Configuration con = HBaseConfiguration.create();

        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(con);

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new HTableDescriptor("jsonTest");
        tableDescriptor.addFamily(new HColumnDescriptor("text"));
        tableDescriptor.addFamily(new HColumnDescriptor("user"));
        tableDescriptor.addFamily(new HColumnDescriptor("entities"));
        
        admin.createTable(tableDescriptor);
        
        HTable hTable = new HTable(con, "jsonTest");
        
        
        
        
        //p.add(Bytes.toBytes("text"),Bytes.toBytes("ActualTweet"),Bytes.toBytes("haha"));
        // The name of the file to open.
        String fileName = "/home/training/hbase/JsonData/twitter.json";

        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);
            int i=0;
            while((line = bufferedReader.readLine()) != null) {
            		
            		Put p = new Put(Bytes.toBytes((i+1)));
            		
            		//JSONObject jsonObject=new JSONObject();
            		JSONObject obj = new JSONObject(line);
            		String actualTweet=obj.getString("text");
            		p.add(Bytes.toBytes("text"),Bytes.toBytes("ActualTweet"),Bytes.toBytes(actualTweet));
            		
            		//user.friends_count and user.name
            		int friends_count=obj.getJSONObject("user").getInt("friends_count");
            		p.add(Bytes.toBytes("user"),Bytes.toBytes("friends_count"),Bytes.toBytes(friends_count));
            		String user_name=obj.getJSONObject("user").getString("name");
            		p.add(Bytes.toBytes("user"),Bytes.toBytes("name"),Bytes.toBytes(user_name));
            		
            		//entities.user_mentions
            		JSONObject entitiesObj=obj.getJSONObject("entities");
            		JSONArray user_mentions=entitiesObj.getJSONArray("user_mentions");
            		//System.out.println("User mentions are:");
            		for(int aInd=0;aInd<user_mentions.length();aInd++)
            		{
            			String user_mention=user_mentions.getJSONObject(aInd).getString("name");
            			String user_mention_column="user_mention_"+(aInd+1);
            			p.add(Bytes.toBytes("entities"),Bytes.toBytes(user_mention_column),Bytes.toBytes(user_mention));
            		}
            		
            		//entities.hashtags
            		JSONArray hashtags=entitiesObj.getJSONArray("hashtags");
            		//System.out.println("Hashtags are:");
            		for(int aInd=0;aInd<hashtags.length();aInd++)
            		{
            			String hashtag=hashtags.getJSONObject(aInd).getString("text");
            			String hashtag_column="hashtag"+(aInd+1);
            			p.add(Bytes.toBytes("entities"),Bytes.toBytes(hashtag_column),Bytes.toBytes(hashtag));
            		}
            		hTable.put(p);
            		
            	
            		i++;
            		
            		
            		
            		
            		//JSONObject
            		
            		//Put p = new Put(Bytes.toBytes(Integer.toString(i)));
                //p.add(Bytes.toBytes("text"),Bytes.toBytes(""),Bytes.toBytes(textSplitAnother[0]));
                //hTable.put(p);
            		
            	}
                
                
            

            // Always close files.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
            // Or we could just do this: 
            // ex.printStackTrace();
        }
        finally
        {
        	hTable.close();
        }
       
        
       /* JSONObject obj = new JSONObject("{interests : [{interestKey:Dogs}, {interestKey:Cats}]}");

       
        JSONArray array = obj.getJSONArray("interests");
        for(int i = 0 ; i < array.length() ; i++){
            System.out.println(array.getJSONObject(i).getString("interestKey"));}
        */
        
    }
}
