import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class Queries {
	public static void main(String args[]) throws IOException {
		Configuration config = HBaseConfiguration.create();

		// Instantiating HTable class
		HTable hTable = new HTable(config, "jsonTest");
		ResultScanner rs;
		Scan scan = new Scan();
		Result r;
		// Trying to deserialize

		// //Query 1---Find out all the users with more than 100 followers
		/*
		  SingleColumnValueFilter sf=new
		  SingleColumnValueFilter(Bytes.toBytes("user"
		  ),Bytes.toBytes("friends_count"),CompareFilter.CompareOp.GREATER,new
		  BinaryComparator(Bytes.toBytes(100)));
		  
		  scan.setFilter(sf); rs=hTable.getScanner(scan);
		  for( r=rs.next();r!=null;r=rs.next()) {
		  
		  for(KeyValue kv:r.raw()) { if(kv.getKeyString().contains("username"))
		  System.out.println(Bytes.toString(kv.getValue()));
		  
		  } }
		 
		 */
		// /Query 2--- Find people who have mentioned more than 10 people in a
		// tweet

		/*
		rs = hTable.getScanner(scan);
		System.out.println("Users who have mentioned 10 users are:");
		for (r = rs.next(); r != null; r = rs.next()) {

			String user_name = "";
			int mention_count = 0;
			for (KeyValue kv : r.raw()) {
				
				if ((kv.getKeyString()).contains("user")
						&& (kv.getKeyString()).contains("name")) {
					user_name = Bytes.toString(kv.getValue());
				}
				if (kv.getKeyString().contains("user_mention")) {
					// System.out.println(user_name);
					mention_count++;
				}
			}
			if (mention_count >= 10) {
				System.out.println(user_name);
			}


		}
		 */
		// /Query 3--- Find total number of tweets containing word "play"
		/*
		SingleColumnValueFilter sf = new SingleColumnValueFilter(
				Bytes.toBytes("text"), Bytes.toBytes("ActualTweet"),
				CompareFilter.CompareOp.EQUAL, new SubstringComparator("play"));
		scan.setFilter(sf);
		rs = hTable.getScanner(scan);
		int countOfTweets = 0;
		for (r = rs.next(); r != null; r = rs.next()) {
			countOfTweets++;
		}
		System.out.println("Number of tweets containing the word 'play' is:"
				+ countOfTweets);

		
		*/
		// /Query 7--- Compare number of tweets of two people
		/*
		String firstPerson = "Laura";
		String secondPerson = "Sarah";
		SingleColumnValueFilter sf1 = new SingleColumnValueFilter(
				Bytes.toBytes("user"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(firstPerson));
		SingleColumnValueFilter sf2 = new SingleColumnValueFilter(
				Bytes.toBytes("user"), Bytes.toBytes("name"),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(secondPerson));
		scan.setFilter(sf1);
		rs = hTable.getScanner(scan);
		int numberOfTweetsFirstPerson = 0;
		int numberOfTweetsSecondPerson = 0;
		int difference = 0;
		for (r = rs.next(); r != null; r = rs.next()) {
			numberOfTweetsFirstPerson++;
		}
		scan.setFilter(sf2);
		rs = hTable.getScanner(scan);
		for (r = rs.next(); r != null; r = rs.next()) {
			numberOfTweetsSecondPerson++;
		}
		if (numberOfTweetsFirstPerson > numberOfTweetsSecondPerson) {
			difference = numberOfTweetsFirstPerson - numberOfTweetsSecondPerson;
			System.out.println(firstPerson + " has " + difference
					+ " more tweets than " + secondPerson);
		} else if (numberOfTweetsSecondPerson > numberOfTweetsFirstPerson) {
			difference = numberOfTweetsSecondPerson - numberOfTweetsFirstPerson;
			System.out.println(secondPerson + " has " + difference
					+ " more tweets than " + firstPerson);
		} else {
			System.out.println("Both persons have same number of tweets");
		}

		*/
		// /Query 4--- hashtags and their count in the descending order
		
		Filter f = new ColumnRangeFilter(Bytes.toBytes("hashtag1"), true,
				Bytes.toBytes("hashtag20"), true);
		scan.setFilter(f);
		rs = hTable.getScanner(scan);
		HashMap<String, Integer> hmap = new HashMap<String, Integer>();
		int test=0;
		for (r = rs.next(); r != null; r = rs.next()) {
			
			for (KeyValue kv : r.raw()) {
				
				if( hmap.containsKey(Bytes.toString(kv.getValue())))
				{
					String key=Bytes.toString(kv.getValue());
					Integer var= hmap.get(key);
					var=var.valueOf(var.intValue()+1);
					hmap.remove(Bytes.toString(kv.getValue()));
					hmap.put(key,var);
				    
				}
				else
				{
					String key=Bytes.toString(kv.getValue());
					Integer var=new Integer(1);
					hmap.put(key,var);
				}
			}
			
		
		}
		hmap=sortByValues(hmap);
		System.out.println("size of hashmap:"+hmap.size());

		Map<String, Integer> map = sortByValues(hmap); 
	      System.out.println("After Sorting:");
	      Set set2 = map.entrySet();
	      Iterator iterator2 = set2.iterator();
	      while(iterator2.hasNext()) {
	           Map.Entry me2 = (Map.Entry)iterator2.next();
	           System.out.print(me2.getKey() + ": ");
	           System.out.println(me2.getValue());
	      }
	      
	}

	
	public static HashMap sortByValues(HashMap map) { 
	       List list = new LinkedList(map.entrySet());
	       // Defined Custom Comparator here
	       Collections.sort(list, new Comparator() {
	            public int compare(Object o1, Object o2) {
	               return ((Comparable) ((Map.Entry) (o2)).getValue())
	                  .compareTo(((Map.Entry) (o1)).getValue());
	            }
	       });

	       // Here I am copying the sorted list in HashMap
	       // using LinkedHashMap to preserve the insertion order
	       HashMap sortedHashMap = new LinkedHashMap();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	              Map.Entry entry = (Map.Entry) it.next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	       return sortedHashMap;
	  }
	
}
//System.out.println(kv.getKeyString());
//System.out.println(Bytes.toString(kv.getValue()));
