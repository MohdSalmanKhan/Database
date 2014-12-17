package com.db.main;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.*;

import test.hive.QueryChecker;

/**
 * Created by root on 24/1/14.
 */

class node
{
        int start_id;
        int end_id;
        int offset;
        int bytesContained;

        node(int start,int end , int off , int b)
        {
            start_id=start;
            end_id=end;
            offset = off;
            bytesContained = b;

        }
        node()
        {

        }
}


class LRU
{
    String tableName;
    String content;
    int pageNumber;
    int position;

    LRU(String t, String c , int p ,int pos)
    {
        tableName = t;
        content = c;
        pageNumber = p;
        position = pos;
    }

    LRU()
    {

    }

}

public class DBSystem
{
    static Map<String , LinkedList<String>> tableAttributes = new HashMap< String , LinkedList<String>>();
    static TreeMap<String,TreeMap<Integer,node>> treeMap=new TreeMap<String,TreeMap<Integer,node>>();
   // static TreeMap<Integer,node> tmp= new TreeMap<Integer, node>();
    static LinkedList<String> ll;
    static FileInputStream fis = null;
    static LRU lru = null;
    static LinkedList<LRU> linkedList;
    static public ReadFile rf;
    static int pos = 0;
    static int firstid=0;
    static Set<String> distinct[] ;//= new HashSet<String>();
    static String configFilePath="";
    
    static Map<String , Set<String>[]> tableDistinctData = new HashMap< String , Set<String>[]>();
    static Map<String , Integer> tableAttributesNumber = new HashMap< String , Integer>();
    static int countNoTables=0;
    static int countNoAttributes;
    static String AttributeName="";
    
    public static void returnAttributeNumber(String tableName,String AttName)
    {
    	AttributeName = tableName+AttName;
    }

    public static void readConfig(String configFilePath)	//working fine
    {

        /* the method is used to read the config file and find out different parameters for the
            project deliverable , this include the maximum number of pages that can reside in main memory
            at any point of time and the page size. This config file also includes the extension for the
            table that contain the records , the config file is also used to obtain the table schema

         */
    	
    	System.out.println("in read config:"+configFilePath);

    	configFilePath = configFilePath;
        rf = new ReadFile();

        try
        {
            RandomAccessFile in = new RandomAccessFile(QueryChecker.configpath,"r");
            String str;

            while ((str = in.readLine()) != null)
            {
                rf.readFile(str);
            }

            in.close();
        }
        catch (IOException e)
        {
            System.out.println("Exception caught:"+e);
        }


        tableAttributes = rf.tableAttributes;

        Iterator itr = tableAttributes.entrySet().iterator();
        
        try{

            while(itr.hasNext())
            {
                Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
                String key = (String) mEntry.getKey();
                ll = new LinkedList<String>();
                ll =  mEntry.getValue();
                Iterator x;
                x = ll.listIterator();
                countNoTables++;
                countNoAttributes=0;
                //System.out.println("Table Name:"+key);
                while(x.hasNext())
                {
                	
                	x.next();
           //        System.out.println(x.next());
                   countNoAttributes++;
                }
                tableAttributesNumber.put(key, countNoAttributes);
            }

        }catch(Exception e){
            System.out.println(e);
        }

    }

    public static void populateDBInfo()
    {
        node obj;
        Iterator itr = tableAttributes.entrySet().iterator();

        try
        {
            while(itr.hasNext())
            {

                int bytesRead=0;
                int pagecount=0;
                int start_record_id=0;
                int end_record_id=0;
                int recordCount = 0;
                int offset = 0;

                Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
                String key = (String) mEntry.getKey();
                TreeMap<Integer,node> tmp= new TreeMap<Integer, node>();
                
                System.out.println("filepath"+rf.pathForData+key);
                fis = new FileInputStream(rf.pathForData+key);
                String Record="";
                int numberOfAttributes=0;
                numberOfAttributes = tableAttributesNumber.get(key);
                //System.out.println("numberOfAtt"+numberOfAttributes);
                //ArrayList<Set<String>> arrayList = new ArrayList<Set<String>>();
                Set<String>distinct1[] = new HashSet[numberOfAttributes];
                for(int i = 0 ; i < distinct1.length ; i++)
                {
                	distinct1[i] = new HashSet<String>();
                }
                
                //System.out.println("len=="+distinct1.length);
                int content;
                int temp=0;
                

                while ((content = fis.read()) != -1)
                {
                    // convert to char and display it
                    //System.out.println((char) content);
                    bytesRead++;
                    Record += (char)content;

                    if((char)content == '\n')
                    {
                        recordCount++;
                        temp=bytesRead;
                        String token[] = Record.split(",");
                        //System.out.println("len="+token.length);
                        for(int i=0;i<token.length;i++)
                        {
                        	
                       // 	System.out.println("token[i]="+token[i]);
                        	distinct1[i].add(token[i]);
                        //	distinct1[i].
                        //	System.out.println("hello");
                        }
                        Record="";
                    }

                    if(bytesRead-1 == (rf.pageSize))
                    {
                        end_record_id = start_record_id + recordCount;
                        pagecount++;
                        end_record_id-=1;
                        if(temp==rf.pageSize+1)
                            temp-=1;
                        
                        obj=new node(start_record_id,end_record_id, offset , temp);
                        tmp.put(pagecount,obj);
                        start_record_id=end_record_id+1;
                        recordCount=0;


                        offset+=(temp);
                        if(temp!=rf.pageSize+1)
                        	temp-=1;
                        bytesRead = rf.pageSize-temp;
                    }
                }
                if(temp!=rf.pageSize)
                {
                    end_record_id=start_record_id+recordCount;
                    end_record_id-=1;
                    bytesRead=temp;
                    obj=new node(start_record_id,end_record_id,offset,temp);
                    pagecount+=1;
                    tmp.put(pagecount,obj);
                }
                treeMap.put(key, tmp);
                tableDistinctData.put(key, distinct1);    
            }

        }
        catch (Exception e)
        {
        	System.out.println("bj");
            e.getStackTrace();
            System.out.println("exception="+e);
        }

    }



    public static int getPageNumber(String tableName , int recordId)
    {
        TreeMap<String,TreeMap<Integer,node>> t = new TreeMap<String, TreeMap<Integer, node>>();
        t=treeMap;

        Iterator it =t.entrySet().iterator();
        node obj = new node();
        while(it.hasNext())
        {
            Map.Entry m = (Map.Entry) it.next();
            TreeMap h = (TreeMap) m.getValue();
            Set keys = h.keySet();
            if( !((String) m.getKey()).equals(tableName))
                continue;

            for(Iterator i = keys.iterator() ; i.hasNext() ;)
            {

                // String k = (String)i.next();

                int n = (Integer) i.next();
                obj=(node) h.get(n);


                if( obj.start_id <= recordId && obj.end_id >= recordId)
                {
                       // System.out.println("pageno="+n);
                        return n;
                }
            }
        }

        return -1;

    }
    
   
    public static String getContent(String tableName , int pageNo )
    {
    	//System.out.println("tname="+tableName+"pageni="+pageNo);
        TreeMap<String,TreeMap<Integer,node>> t = new TreeMap<String, TreeMap<Integer, node>>();
        t=treeMap;

        Iterator it =t.entrySet().iterator();
        node obj = new node();

        int offset=0 , bytesToBeRead = 0;
        int flag = 0;

        int content;

        String returnVal ="";

        while(it.hasNext())
        {
            Map.Entry m = (Map.Entry) it.next();
            TreeMap h = (TreeMap) m.getValue();
            Set keys = h.keySet();

            if( !((String) m.getKey()).equals(tableName))
                continue;

            //System.out.println(m.getKey());
            for(Iterator i = keys.iterator() ; i.hasNext() ;)
            {

                // String k = (String)i.next();
                int n = (Integer) i.next();
                obj=(node) h.get(n);

                if( n == pageNo )
                {
                 //   System.out.println("pageno="+n);
                    offset = obj.offset;
                    bytesToBeRead = obj.bytesContained;
                    flag = 1;
                    firstid=obj.start_id;
                   // System.out.println(n+"-----"+obj.start_id+"-------"+obj.end_id+"----------"+obj.offset+"-----"+obj.bytesContained);
                    break;
                }
            }

            if(flag == 1)
                break;
        }


        try
        {
        	
            RandomAccessFile r  = new RandomAccessFile(rf.pathForData+tableName,"r");
            r.seek(offset);
            //  System.exit(0);
            //System.out.println("size=");
            int bytesRead = 0;


            while ((content = r.readByte() ) != -1)
            {
                // convert to char and display it
                //System.out.println((char) content);
                bytesRead++;

                if(bytesRead == bytesToBeRead)
                    break;

                 returnVal+=(char)content;
            }

        }
        catch (IOException e)
        {    	
            e.printStackTrace();
        }

    //   System.out.println("string="+returnVal);
        return returnVal;


    }

    public static long lastRecordID=0;
    
    public static long lastRecordIdToBeReturned(String tableName)
    {
    	TreeMap<String,TreeMap<Integer,node>> t = new TreeMap<String, TreeMap<Integer, node>>();
        t=treeMap;
       // System.out.println("salman");
        Iterator it =t.entrySet().iterator();
        node obj = new node();
        while(it.hasNext())
        {
            Map.Entry m = (Map.Entry) it.next();
            TreeMap h = (TreeMap) m.getValue();
            Set keys = h.keySet();
            String tname = (String)m.getKey();
            if(tname.compareToIgnoreCase(tableName)!=0)
            	continue;
           //  System.out.println(m.getKey());
            
            for(Iterator i = keys.iterator() ; i.hasNext() ;)
            {


                int n = (Integer) i.next();
                obj=(node) h.get(n);
                if(obj.end_id > lastRecordID)
                	lastRecordID=obj.end_id;
            }
        }

    	return lastRecordID;
    }
    
    
    public static Vector returnColumnNames(String tableName,String filePath)
    {
    	Vector<String> columnNames = new Vector<String>();
    	boolean startOfTableFlag = false;
    	
    	
    	try
    	{
    		
    		RandomAccessFile in = new RandomAccessFile(QueryChecker.configpath,"r");
    		String line = "";
    		
    		while((line=in.readLine()) != null)
    		{
    			line=line.trim();
    			
    			if( startOfTableFlag )
    			{
    				if(line.compareToIgnoreCase("end") == 0)
    				{
    					break;
    				}
    				String tok[] = line.split(",");
    				columnNames.add(tok[0].trim());
    			}
    			
    			
    			if(line.compareToIgnoreCase(tableName) == 0)
    			{
    				startOfTableFlag = true;
    			}
    			
    			
    		
    		}
    		
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	
    
    	
    	return columnNames;
    }
   
    public static int returnFirstID(int recordID,String tableName)
    {
        TreeMap<String,TreeMap<Integer,node>> t = new TreeMap<String, TreeMap<Integer, node>>();
        t=treeMap;
       // System.out.println("salman");
        Iterator it =t.entrySet().iterator();
        node obj = new node();
        
        
      //  System.out.println("recordId="+recordID+"\ttablename="+tableName);
        
        int firstID = 0;
        boolean flag = false;
        
        while(it.hasNext())
        {
            Map.Entry m = (Map.Entry) it.next();
            TreeMap h = (TreeMap) m.getValue();
            Set keys = h.keySet();

          //  System.out.println("1");

    //         System.out.println(m.getKey());
       //      if(!((String)m.getKey()).contains("countries"))
         //   	 continue;
           // System.out.println("2");
            String tname = (String)m.getKey();
            
            if( tname.compareToIgnoreCase(tableName) != 0 )
            	continue;
            
            for(Iterator i = keys.iterator() ; i.hasNext() ;)
            {



                //System.out.println("3");
               // String k = (String)i.next();
               // System.out.println("k="+k);
                int n = (Integer) i.next();
                obj=(node) h.get(n);

                 //System.out.println("pageno="+n+"start="+obj.start_id+" end="+obj.end_id);
                //if(obj.end_id > lastRecordID)
                //	lastRecordID=obj.end_id;

            //     System.out.println(n+"-----"+obj.start_id+"-------"+obj.end_id+"----------"+obj.offset+"-----"+obj.bytesContained);
          
                if( recordID >= obj.start_id && recordID <= obj.end_id )
                {
                //	  System.out.println(n+"-----1"+obj.start_id+"-------2"+obj.end_id+"----------3"+obj.offset+"-----4"+obj.bytesContained);
                	firstID = obj.start_id;
                	flag = true;
                	break;
                }
                
                
            }
            
            if(flag)
            	break;
        }
        //System.out.println("BYE");
    
   //     System.out.println("firstIdinDb="+firstID);

    return firstID;
    	
    }

    public static int returnLastID(String tableName)
    {
        TreeMap<String,TreeMap<Integer,node>> t = new TreeMap<String, TreeMap<Integer, node>>();
        t=treeMap;
       
        Iterator it =t.entrySet().iterator();
        node obj = new node();
        int endID = 0;
        while(it.hasNext())
        {
            Map.Entry m = (Map.Entry) it.next();
            TreeMap h = (TreeMap) m.getValue();
            Set keys = h.keySet();
           // System.out.println(m.getKey());
            
            if( (tableName+".csv").compareToIgnoreCase((String)m.getKey()) != 0 )
            	continue;
            
            for(Iterator i = keys.iterator() ; i.hasNext() ;)
            {
                int n = (Integer) i.next();
                obj=(node) h.get(n);
                endID = obj.end_id;
                 
            }
        }
        
        return endID;
    }
    
    
    
    public static void printMap()
    {
        TreeMap<String,TreeMap<Integer,node>> t = new TreeMap<String, TreeMap<Integer, node>>();
        t=treeMap;
       // System.out.println("salman");
        Iterator it =t.entrySet().iterator();
        node obj = new node();
        while(it.hasNext())
        {
            Map.Entry m = (Map.Entry) it.next();
            TreeMap h = (TreeMap) m.getValue();
            Set keys = h.keySet();

          //  System.out.println("1");

    //         System.out.println(m.getKey());
       //      if(!((String)m.getKey()).contains("countries"))
         //   	 continue;
           // System.out.println("2");
             System.out.println(m.getKey());
            for(Iterator i = keys.iterator() ; i.hasNext() ;)
            {



                //System.out.println("3");
               // String k = (String)i.next();
               // System.out.println("k="+k);
                int n = (Integer) i.next();
                obj=(node) h.get(n);

                 //System.out.println("pageno="+n+"start="+obj.start_id+" end="+obj.end_id);
                //if(obj.end_id > lastRecordID)
                //	lastRecordID=obj.end_id;

                 System.out.println(n+"-----"+obj.start_id+"-------"+obj.end_id+"----------"+obj.offset+"-----"+obj.bytesContained);
                 
            }
        }
        System.out.println("BYE");
    }


    static LinkedList<LRU> linkedList1 = new LinkedList<LRU>();
    
    
    public static String getAllrecord(String tableName, int recordId)
    {
    	   String retval="";
           tableName=tableName+".csv";
           LRU add = new LRU();
           String returnValue;
           int count=0;
           String value="";
           int numberstringtobefetched=0;
           int pageNo = getPageNumber(tableName,recordId);
           //System.out.println("pageno="+pageNo);
         //  System.out.println("pagenum="+pageNo);

           if(pageNo == -1) {
               System.out.println("No such recordId/tablename");
           }
           else {
               if (linkedList1.isEmpty()) {
                   value = getContent(tableName, pageNo);

                   numberstringtobefetched=recordId-firstid;
                   String token[] = value.split("\n");
                   retval=token[numberstringtobefetched];
                   lru = new LRU(tableName, value, pageNo, pos);

                   pos++;
                   Integer number = lru.position;
                   String strArpit = "MISS " + number;
                 //  System.out.println(strArpit);
               //    System.out.println(Integer.toString(lru.position));
                   linkedList1.add(lru);
               } else {
                   int flag = 0;

                   Iterator<LRU> x;
                   x = linkedList1.listIterator();

                   while (x.hasNext()) {

                       LRU new1 = new LRU();
                       new1 = x.next();


                       if ((new1.tableName).compareToIgnoreCase(tableName) == 0) {

                           if (new1.pageNumber == pageNo) {
                               //        System.out.println("1");
                               flag = 1;
                               add = new1;
                               linkedList1.remove(add);
                               returnValue = "HIT";
               //                System.out.println("HIT");
                               break;
                           }

                       }

                       count++;

                   }

                   //System.out.println("count="+count);

                   if (flag == 0) {

             //      	System.out.println("tablename=="+pageNo);
                       value = getContent(tableName, pageNo);
                      // System.out.println("1st id="+firstid+"recordid="+recordId);
                       numberstringtobefetched = recordId-firstid;
                       String token[] = value.split("\n");
                       retval=token[numberstringtobefetched];
            //           System.out.println("stringreturn"+retval);
                      // if(recordId == 41){
                       //    System.out.println("----"+token[0]+" "+token[1]+" "+token[2]);
                       //}

                       if (count == rf.numOfPages) {
                           LRU l = new LRU();

                           l = linkedList1.getFirst();
                   //        System.out.println("MISS " + l.position);
                           returnValue = "MISS" + l.position;
                           linkedList1.removeFirst();
                           lru = new LRU(tableName, getContent(tableName, pageNo), pageNo, l.position);
                           linkedList1.addLast(lru);

                       } else {
                           returnValue = "MISS";
                           lru = new LRU(tableName, getContent(tableName, pageNo), pageNo, pos);
                           linkedList1.addLast(lru);
                 //          System.out.println("MISS " + lru.position);
                           pos++;

                       }

                   } else if (flag == 1) {
                       value = getContent(tableName, pageNo);
                       String token[] = add.content.split("\n");
                       numberstringtobefetched = recordId-firstid;
            //           System.out.println(numberstringtobefetched+" num");

                       retval=token[numberstringtobefetched];
                       linkedList1.addLast(add);
                   }
               }
           }

          // System.out.println(numberstringtobefetched);
           //System.out.println("record is="+retval);
          // if(recordId == 41)
        //   System.out.println("value="+value);

           
           return value;

    	
    	
//    	return null;
    }

    public static String getRecord(String tableName, int recordId)
    {

       // printMap();

        //System.out.println("heyyy");



        String retval="";
        tableName=tableName+".csv";
        LRU add = new LRU();
        String returnValue;
        int count=0;
        int numberstringtobefetched=0;
        int pageNo = getPageNumber(tableName,recordId);
        //System.out.println("pageno="+pageNo);
      //  System.out.println("pagenum="+pageNo);

        if(pageNo == -1) {
            System.out.println("No such recordId/tablename");
        }
        else {
            if (linkedList1.isEmpty()) {
                String value = getContent(tableName, pageNo);

                numberstringtobefetched=recordId-firstid;
                String token[] = value.split("\n");
                retval=token[numberstringtobefetched];
                lru = new LRU(tableName, value, pageNo, pos);

                pos++;
                Integer number = lru.position;
                String strArpit = "MISS " + number;
              //  System.out.println(strArpit);
            //    System.out.println(Integer.toString(lru.position));
                linkedList1.add(lru);
            } else {
                int flag = 0;

                Iterator<LRU> x;
                x = linkedList1.listIterator();

                while (x.hasNext()) {

                    LRU new1 = new LRU();
                    new1 = x.next();


                    if ((new1.tableName).compareToIgnoreCase(tableName) == 0) {

                        if (new1.pageNumber == pageNo) {
                            //        System.out.println("1");
                            flag = 1;
                            add = new1;
                            linkedList1.remove(add);
                            returnValue = "HIT";
            //                System.out.println("HIT");
                            break;
                        }

                    }

                    count++;

                }

                //System.out.println("count="+count);

                if (flag == 0) {

            //    	System.out.println("tablename=="+pageNo);
                    String value = getContent(tableName, pageNo);
                   // System.out.println("1st id="+firstid+"recordid="+recordId);
                    numberstringtobefetched = recordId-firstid;
                    String token[] = value.split("\n");
                    retval=token[numberstringtobefetched];
                    System.out.println("stringreturn"+retval);
                   // if(recordId == 41){
                    //    System.out.println("----"+token[0]+" "+token[1]+" "+token[2]);
                    //}

                    if (count == rf.numOfPages) {
                        LRU l = new LRU();

                        l = linkedList1.getFirst();
                //        System.out.println("MISS " + l.position);
                        returnValue = "MISS" + l.position;
                        linkedList1.removeFirst();
                        lru = new LRU(tableName, getContent(tableName, pageNo), pageNo, l.position);
                        linkedList1.addLast(lru);

                    } else {
                        returnValue = "MISS";
                        lru = new LRU(tableName, getContent(tableName, pageNo), pageNo, pos);
                        linkedList1.addLast(lru);
              //          System.out.println("MISS " + lru.position);
                        pos++;

                    }

                } else if (flag == 1) {
                    String value = getContent(tableName, pageNo);
                    String token[] = add.content.split("\n");
                    numberstringtobefetched = recordId-firstid;
         //           System.out.println(numberstringtobefetched+" num");

                    retval=token[numberstringtobefetched];
                    linkedList1.addLast(add);
                }
            }
        }

       // System.out.println(numberstringtobefetched);
        //System.out.println("record is="+retval);
       // if(recordId == 41)
      //      System.out.println("value="+retval);

        return retval;
    }

        public static void insertRecord(String tableName, String record) throws UnsupportedEncodingException {

        tableName+=".csv";
        TreeMap<String,TreeMap<Integer,node>> t = new TreeMap<String, TreeMap<Integer, node>>();
        t=treeMap;
        record+="\n";
        Iterator it =t.entrySet().iterator();
        node obj = new node();
        int newOffset=0 , newBytesContained = 0 , newStartId = 0 , newEndId = 0;
        int n = 0;
        while(it.hasNext())
        {
            Map.Entry m = (Map.Entry) it.next();
            TreeMap h = (TreeMap) m.getValue();
            Set keys = h.keySet();

            if( !((String) m.getKey()).equals(tableName))
                continue;

            for(Iterator i = keys.iterator() ; i.hasNext() ;)
            {

                n = (Integer) i.next();
                obj=(node) h.get(n);
                newOffset = obj.offset;
                newBytesContained = obj.bytesContained;
                newStartId = obj.end_id;
            }


        }


        final byte[] utf16Bytes = record.getBytes("UTF-16");
        //System.out.println(utf16Bytes.length);



        int flag = 0;
        if(obj.bytesContained + utf16Bytes.length <= rf.pageSize)
        {
            flag=0;
            Iterator<LRU> x;
            x = linkedList1.listIterator();

            while(x.hasNext())
            {

                LRU new1 = new LRU();
                new1 = x.next();


                if((new1.tableName).compareToIgnoreCase(tableName)==0 )
                {

                    if( new1.pageNumber == n)
                    {
                        new1.content+=record;
                        flag = 1;
                        break;

                    }

                }
            }
            if(flag == 0)
            {
                if(linkedList1.isEmpty())
                {
                    String value= getContent(tableName,n);
                    value+=record;
                    lru = new LRU(tableName,value,n,0);
                    pos++;
                    linkedList1.add(lru);
                }
                else
                {
                    if(linkedList1.size() == rf.numOfPages)
                    {
                        //linkedList1.removeFirst();
                        String value= getContent(tableName,n);
                        value+=record;
                        lru = new LRU(tableName,value,n,linkedList1.getFirst().position);
                        linkedList1.removeFirst();
                        linkedList1.addLast(lru);
                    }
                    else
                    {
                        String value= getContent(tableName,n);
                        value+=record;
                        lru = new LRU(tableName,value,n,pos);
                        pos++;
                        linkedList1.addLast(lru);
                    }
                }
            }


        }
        else
        {
           // linkedList1.addLast(obj);
            /*flag=0;
            Iterator<LRU> x;
            x = linkedList1.listIterator();
            while(x.hasNext())
            {
                LRU new1 = new LRU();
                new1 = x.next();
                if((new1.tableName).compareToIgnoreCase(tableName)==0 )
                {

                    if( new1.pageNumber == n)
                    {
                       // new1.content+=record;
                        flag = 1;
                        break;
                    }
                }
            }
            if(flag == 0)
            {
                if(linkedList1.isEmpty())
                {
                    String value= getContent(tableName,n);
                 //   value+=record;
                    lru = new LRU(tableName,value,n,0);
                    pos++;
                    linkedList1.add(lru);
                }
                else
                {
                    if(linkedList1.size() == rf.numOfPages)
                    {
                        //linkedList1.removeFirst();
                        String value= getContent(tableName,n);
                       // value+=record;
                        lru = new LRU(tableName,value,n,linkedList1.getFirst().position);
                        linkedList1.removeFirst();
                        linkedList1.addLast(lru);
                    }
                    else
                    {
                        String value= getContent(tableName,n);
                       // value+=record;
                        lru = new LRU(tableName,value,n,pos);
                        pos++;
                        linkedList1.addLast(lru);
                    }
                }
            }


            */


            flag=0;
          //  System.out.println("newoffset="+newOffset);
            newOffset+= newBytesContained;
            newBytesContained = utf16Bytes.length;
            newStartId+=1;
            newEndId=newStartId;
           // System.out.println("newoffset="+newOffset);
        //    TreeMap<Integer,node> tmp= new TreeMap<Integer, node>();

            obj = new node(newStartId,newEndId,newOffset,newBytesContained);
            TreeMap<Integer,node> tmp1= new TreeMap<Integer, node>();
            tmp1 = treeMap.get(tableName); 
            
            tmp1.put(n+1,obj);

          //  printMap();
            treeMap.put(tableName,tmp1);
           // System.out.println("------------------------");
          //  printMap();
            //LRU l = new LRU(tableName,record,n+1);

            if(linkedList1.isEmpty())
            {


                lru = new LRU(tableName,record,n+1,0);
                pos++;
                linkedList1.add(lru);
            }
            else
            {
                if(linkedList1.size() == rf.numOfPages)
                {


                    lru = new LRU(tableName,record,n+1,linkedList1.getFirst().position);
                    linkedList1.removeFirst();
                    linkedList1.addLast(lru);
                }
                else
                {

                    lru = new LRU(tableName,record,n+1,pos);
                    pos++;
                    linkedList1.addLast(lru);
                }
            }


        }


        try
        {
            RandomAccessFile r  = new RandomAccessFile(rf.pathForData+tableName,"rw");

            r.seek(r.length());


            r.write(record.getBytes());


        }
        catch (Exception e)
        {
            System.out.println(e);
        }

        //populateDBInfo();



        //System.out.println("string="+returnVal);
    }
}
