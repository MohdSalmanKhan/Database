package test.hive;


import com.db.main.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


/*
 * Date: 12-9-13
 */

import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TJoinItem;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.stmt.*;

public class tutorial {
	static TGSqlParser sqlparser;
	static public DBSystem db;
	static LinkedList<String> ll;
	static LinkedHashSet <String> hs = new LinkedHashSet<String>();
	static HashSet<String> tableName =new HashSet<String>();
	static HashMap<String,String> aliasingHash = new HashMap<String,String>();
	static HashMap<String,String> reverseAliasingHash = new HashMap<String,String>();
	static String tableNamesInTheQuery[] = new String[1000];
	static int countForNumOfTables = 0;
	static HashMap<String,LinkedList<String>> hashMapForAliasing = new HashMap<String , LinkedList<String>>();
	static boolean invalidQueryFlag = false;
	public static String tbName="";
	public static String whereClauseValueToDbSystem="";
	public static String columnValueToDbSystem="";
	public static String orderByClauseValueToDbSystem="";
	public static int valueToDbSystem=0;
	public static String table1="",table2="",joiningCondition="",configFile="";
	public static boolean flagForCheckingJoin=false;
	public static String tableNamesInJoin[] = new String[100];
	public static int tableCounter = 0;
	
	
	public static boolean checkForColumnNameInAliasing(String columnName)
	{	
		
		//String tokenAliasing[] = columnName.split(".");
		//System.out.println(tokenAliasing[0]);
		Iterator itr = db.rf.tableAttributes.entrySet().iterator();
        boolean flag=false;
        String TableName,clmName;
        int i;
        for(i=0;i<columnName.length();i++)
        {
        	if(columnName.charAt(i)=='.')
        	{
        		break;
        	}
        }
        TableName=columnName.substring(0,i);
        clmName=columnName.substring(i+1, columnName.length());
        
        System.out.println("column name is:"+clmName);
        
    //    System.out.println("\nTablename="+TableName);
     //  System.out.println("clmname="+clmName);
       TableName = aliasingHash.get(TableName);
       TableName = TableName + ".csv";
       
       System.out.println("tablename is:"+TableName);
       
       	while(itr.hasNext())
        {
            Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
            String key = (String) mEntry.getKey();
            //System.out.println("key is="+key);
            if(key.compareToIgnoreCase(TableName)!=0)
            	continue;
            ll = new LinkedList<String>();
            ll =  mEntry.getValue();
            Iterator x;
            x = ll.listIterator();
           
           // System.out.println("Table Name:"+key);
            while(x.hasNext())
            {
               String st=(String)x.next();
               String token[]=st.split(",");
        //       System.out.println("token[0]="+token[0]);
               if(token[0].trim().compareToIgnoreCase(clmName)==0)
               {
            		  flag=true;
            		  break;
               }               
              //  System.out.println("string="+st);
            }
            if(flag==true)
         	   break;
          
        }		
		return flag;		
	
		
		
	}
	
	public static boolean checkForColumnName(String columnName)
	{
		
		Iterator itr = db.rf.tableAttributes.entrySet().iterator();
        boolean flag=false;
        
       	while(itr.hasNext())
        {
            Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
            String key = (String) mEntry.getKey();
            ll = new LinkedList<String>();
            ll =  mEntry.getValue();
            Iterator x;
            x = ll.listIterator();
           // System.out.println("Table Name:"+key);
            while(x.hasNext())
            {
               String st=(String)x.next();
               String token[]=st.split(",");
               if(token[0].trim().compareToIgnoreCase(columnName)==0)
               {
            		  flag=true;
            		  break;
               }               
              //  System.out.println("string="+st);
            }
            if(flag==true)
         	   break;
          
        }		
		return flag;		
	}
	public static String dataTypeReturnValueInAliasing(String columnName)
	{
		Iterator itr = db.rf.tableAttributes.entrySet().iterator();
        String dataTypeValue="";
        boolean flag=false;
        String TableName,clmName;
        int i;
        for(i=0;i<columnName.length();i++)
        {
        	if(columnName.charAt(i)=='.')
        	{
        		break;
        	}
        }
        TableName=columnName.substring(0,i);
       // System.out.println("colmname"+columnName);
        clmName=columnName.substring(i+1, columnName.length());
        TableName = aliasingHash.get(TableName);
        TableName = TableName + ".csv";
        while(itr.hasNext())
        {
            Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
            String key = (String) mEntry.getKey();
            if(!tableName.contains(key))
            	continue;
            if(key.compareToIgnoreCase(TableName)!=0)
            	continue;
            ll = new LinkedList<String>();
            ll =  mEntry.getValue();
            Iterator x;
            x = ll.listIterator();
            
           // System.out.println("Table Name:"+key);
            while(x.hasNext())
            {
               String st=(String)x.next();
               String token[]=st.split(",");
               if(token[0].toLowerCase().trim().compareToIgnoreCase(clmName.toLowerCase().trim())==0)
               {
            		   dataTypeValue = token[1].trim().toLowerCase();
            		   flag=true;
            		   break;
               }
             //  System.out.println("string="+st);
            }

            if(flag==true)
         	   break;
        }		
       // System.out.println("hello");
		return dataTypeValue;

		
		
		
		
	}
	public static String dataTypeReturnValue(String columnName)
	{
		
		Iterator itr = db.rf.tableAttributes.entrySet().iterator();
        String dataTypeValue="";
        boolean flag=false;
       	while(itr.hasNext())
        {
            Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
            String key = (String) mEntry.getKey();
            if(!tableName.contains(key))
            	continue;
            ll = new LinkedList<String>();
            ll =  mEntry.getValue();
            Iterator x;
            x = ll.listIterator();
           // System.out.println("Table Name:"+key);
            while(x.hasNext())
            {
               String st=(String)x.next();
               String token[]=st.split(",");
               if(token[0].trim().compareToIgnoreCase(columnName)==0)
               {
            		   dataTypeValue = token[1].trim().toLowerCase();
            	//	   System.out.println("Inreturntype="+dataTypeValue+"key="+key+"st="+st);
            		   flag=true;
            		   break;
               }
             //  System.out.println("string="+st);
            }

            if(flag==true)
         	   break;
        }		
       //	System.out.println("Inreturntype="+dataTypeValue);
		return dataTypeValue;
		
		
	}
	
	public static int getNumberOfCountsOfTheTablesInWhichColumnExist(String columnName)
	{
		Iterator itr = db.rf.tableAttributes.entrySet().iterator();
        int count=0;
       	while(itr.hasNext())
        {
            Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
            String key = (String) mEntry.getKey();
            if(!tableName.contains(key))
            	continue;
       //     System.out.println("key="+key);
            ll = new LinkedList<String>();
            ll =  mEntry.getValue();
            Iterator x;
            x = ll.listIterator();
           // System.out.println("Table Name:"+key);
            while(x.hasNext())
            {
               String st=(String)x.next();
               String token[]=st.split(",");
               if(token[0].trim().compareTo(columnName)==0)
               {
            		  count++;
               }               
              //  System.out.println("string="+st);
            }
          
        }
       	return count;
	}
	
	
	public static boolean validator(String validateString)
	{
		String operator="";
		boolean flag=false;
		Iterator iterator = hs.iterator(); 
	    String dataType="";
	    float fnumber;
	    int inumber;
	    String str;
	    int number;
	    char dataValue;
	      // check values
	    
	      while (iterator.hasNext())
	      {
	         //System.out.println("Value: "+iterator.next() + " ");
	    	operator=(String)iterator.next();
	    //	operator = operator.toLowerCase();
	    	if(validateString.contains(operator))
	    	{
	    		flag=true;
	    		break;
	    	}  
	      }
	      if(flag==false)
	      {
	    	  //System.out.println("1");
	    	  return false;
	      }
	      else
	      {
	    	  
	    	  String token[] = validateString.split(operator);
	    	  
	    	//  System.out.println("token[0]="+token[0]+"\ttoken[1]=");
	    	  boolean columnExistValue=false;
	    	  String columnChecker=token[0];
	    	  String valueChecker=token[1];
	    	  
	    	  if(token[0].contains("."))
	    	  {
	    		//System.out.println("--->>>>>>>>>>>>"+token[0]);  
	    		columnExistValue = checkForColumnNameInAliasing(token[0].trim());  
	    	  }
	    	  else
	    	  {
	    		  columnExistValue = checkForColumnName(token[0].trim());
	    	  }
	    	  {
	    		  if(columnExistValue==true)
	    		  {//	  System.out.println("token="+token[0]);
	    			  if(!token[0].contains("."))
	    			  {
	    				 // System.out.println("a");
	    				  number = getNumberOfCountsOfTheTablesInWhichColumnExist(token[0].trim());
	    				  if(number>1)
	    					  return false;
	    			  }
	    			  if(token[1].contains("."))
	    	    	  {
	    	    		//System.out.println("--->>>>>>>>>>>>"+token[0]);  
	    	    		columnExistValue = checkForColumnNameInAliasing(token[1].trim());  
	    	    	  }
	    	    	  else
	    	    	  {
	    	    		  columnExistValue = checkForColumnName(token[1].trim());
	    	    	  }  
	    			  if(columnExistValue==true)
	    			  {
	    				  String dataType1,dataType2;
	    				  if(!token[1].contains("."))
	    				  {
	    					 // System.out.println("b");
	    					  number = getNumberOfCountsOfTheTablesInWhichColumnExist(token[1].trim());
		    		  		  if(number>1)
		    		  			  return false;
	    				  }
	    				  
	    				  if(token[0].contains("."))
	    	    		  {
	    	    			  dataType1 = dataTypeReturnValueInAliasing(token[0].trim());
	    	    		  }
	    	    		  else
	    	    		  {
	    	    			  dataType1 = dataTypeReturnValue(token[0].trim());
	    	    		  }
	    				  if(token[1].contains("."))
	    	    		  {
	    		//			  System.out.println("token="+token[1]);
	    	    			  dataType2 = dataTypeReturnValueInAliasing(token[1].trim());
	    	    		  }
	    	    		  else
	    	    		  {
	    	    			  dataType2 = dataTypeReturnValue(token[1].trim());
	    	    		  }
	    				//  System.out.println("datatype1="+dataType1+"datatype2="+dataType2);
	    				  if((dataType1.compareTo(dataType2)==0) || (dataType1.contains("varchar") && dataType2.contains("varchar")) )
	    				  {
	    					  return true;
	    				  }
	    				  else
	    				  {
	    				//	  System.out.println("kkk");
	    					  return false;
	    				  }
	    			  }
	    			  else
	    			  {	    				  
	    				  columnExistValue =true;
	    			  }
	    		  }
	    		  if(columnExistValue==false)
	    		  {
	    			  columnChecker=token[1];
	    			  valueChecker=token[0];
	    			  if(token[1].contains("."))
	    	    	  {
	    	    		//System.out.println("--->>>>>>>>>>>>"+token[0]);  
	    	    		columnExistValue = checkForColumnNameInAliasing(token[1].trim());  
	    	    	  }
	    	    	  else
	    	    	  {
	    	    		  columnExistValue = checkForColumnName(token[1].trim());
	    	    	  }  
	    		  }
	    		  if(columnExistValue==false)
	    			  return false;	    		 
	    		  
	    		  if(columnChecker.contains("."))
	    		  {
	    			  dataType = dataTypeReturnValueInAliasing(columnChecker.trim());
	    		  }
	    		  else
	    		  {
	    		//	  System.out.println("columnchecker="+columnChecker);
	    			  dataType = dataTypeReturnValue(columnChecker.trim());
	    		//	  System.out.println("datatype="+dataType);
	    		  }
	    //		  System.out.println("datatype="+dataType);
	    		  valueChecker=valueChecker.trim();
	    	//	  System.out.println("value="+valueChecker);
	    		  try
	    		  {
	    	//		  System.out.println("d1");
    				  inumber=Integer.parseInt(valueChecker);
    		//		  System.out.println("number="+valueChecker);
    				  dataValue='i';
	    		  }
	    		  catch(Exception e1)
	    		  {
	    			  try
	    			  {
	    	//			  System.out.println("d2");
	    				  fnumber=Float.parseFloat(valueChecker);
	  	    			  dataValue='f';
	    			  }
	    			  catch(Exception e2)
	    			  {
	    //				  System.out.println("d3");
	    				  dataValue = 's';
	    			  }
	    		  }	    		  
	    	  }
	    	  if((valueChecker.charAt(0)=='"' && valueChecker.charAt(valueChecker.length()-1)=='"') || (valueChecker.charAt(0)=='\'' && valueChecker.charAt(valueChecker.length()-1)=='\''))
	    	  {	  
	    		  if(dataValue == 's' && ( dataType.toLowerCase().contains("varchar") || dataType.toLowerCase().contains("string") ))
	    		  {
	    			  if(dataType.toLowerCase().contains("varchar"))
	    			  {
	    				  int offsetStartOfBracket = dataType.indexOf("(");
		    			  int offsetEndOFBracket = dataType.indexOf(")");
		    			  String maxValueOfTheVarcharDataType = dataType.substring(offsetStartOfBracket+1, offsetEndOFBracket);
		    			  int maxLengthOfTheDataType = Integer.parseInt(maxValueOfTheVarcharDataType);
		    			  int lengthOfTheString = valueChecker.length();
		    		  
		    			  if(lengthOfTheString > maxLengthOfTheDataType )
		    			  {
		    				//  System.out.println("3");
		    				  return false;
		    			  }
		    			  else
		    				  return true;
		    		  }
		    		  else
		    		  {
		    			  return true;
		    		  }		    		  
		    	  }
	    		  else
	    			  return false;
	    	  }
	    	  else if(dataValue == 'i' && dataType.toLowerCase().contains("integer"))
	    	  {
	    		  return true;
	    	  }
	    	  else if(dataValue == 'f' && dataType.toLowerCase().contains("float"))
	    	  {
	    		  return true;
	    	  }
	    	  else
	    	  {	//	System.out.println("4");	
	    		 // 	System.out.println("datavalue="+dataValue+"datatype="+dataType);
	    		  	return false;
	    		  
	    	  }
	      }
		
	}
	
	public static String printTheColumnsForStarQuery()
	{
    	
        Iterator itr = db.rf.tableAttributes.entrySet().iterator();
        
        LinkedList<String> linkedListForStarQuery;
        
        LinkedList<String> tempLinkedList;
        
        String returnValue="";

        HashSet<String> columns =new HashSet<String>();
        
    	while(itr.hasNext())
        {
            Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
            String key = (String) mEntry.getKey();
            if(!tableName.contains(key))
            	continue;
            
            ll = new LinkedList<String>();
            ll =  mEntry.getValue();
            Iterator x;
            x = ll.listIterator();
           // System.out.println("Table Name:"+key);
            while(x.hasNext())
            {
               String st=(String)x.next();
               //System.out.println("string="+st);
               String tok[] = st.split(",");
               
               if(columns.contains(tok[0].trim()))
            	   continue;
               
               returnValue += tok[0].trim() + ",";
               
               columns.add(tok[0].trim());	   
               
               
             /*  if(hashMapForAliasing.containsKey(tok[0].trim()))
               {
            	   tempLinkedList = hashMapForAliasing.get(tok[0].trim());
            	   tempLinkedList.add(key);
            	   hashMapForAliasing.put(tok[0].trim(), tempLinkedList);
               }
               else
               {
            	   linkedListForStarQuery = new LinkedList<String>();
            	   linkedListForStarQuery.add(key);
            	   hashMapForAliasing.put(tok[0].trim(), linkedListForStarQuery);
               }
               
               */
               
            }
        }
    	/*
    	 Iterator it = hashMapForAliasing.entrySet().iterator();
    	 
    	 while(it.hasNext())
         {
             Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) it.next();
             String key = (String) mEntry.getKey();
             linkedListForStarQuery = new LinkedList<String>();
             linkedListForStarQuery =  mEntry.getValue();
             Iterator x;
             x = linkedListForStarQuery.listIterator();
             
             int count = 0;
             
          //   System.out.println("Column Name:"+key);
             while(x.hasNext())
             {
                String st=(String)x.next();
            //    System.out.println("string1="+st);
                count++;
             }
             
             if(count>1)
             {
            	 x = linkedListForStarQuery.listIterator();
            	 while(x.hasNext())
                 {
                    String st=(String)x.next();
                    int dotOffset = st.indexOf(".");
                    st = st.substring(0,dotOffset);
                    
                   // System.out.println("=-------string1="+st);
                    if(reverseAliasingHash.containsKey(st))
                    	returnValue += (reverseAliasingHash.get(st)+"."+key+",");
                    else
                    	returnValue += (st+"."+key+",");
                  
                    
                    
                 }
             }
             else if(count == 1)
             {
            	
            	 x = linkedListForStarQuery.listIterator();
            	 while(x.hasNext())
                 {
                    String st=(String)x.next();
                    int dotOffset = st.indexOf(".");
                    st = st.substring(0,dotOffset);
                    
                    if(reverseAliasingHash.containsKey(st))
                    	returnValue += (reverseAliasingHash.get(st)+"."+key+",");
                    else
                    	returnValue += (key+",");
                    
              
                    
                    
                 }
             }
            	 
         }
    	 */
    	 returnValue = returnValue.substring(0,returnValue.length()-1);
    	 
    	 return returnValue;
     	 
    	

	}
	
	
    public static void selectMethod(String query,String configpath)
    {

    	invalidQueryFlag = false;
    	db = new DBSystem();
    	//db.readConfig("/home/salman/workspace/Database/src/com/db/main/config.txt");
    	System.out.println("in selectMethod:"+configpath);
    	db.readConfig(configpath);
    	configFile = configpath;

    	hs.add("<>");
    	hs.add(">=");
    	hs.add("!>");
    	hs.add(">");
    	hs.add("<=");
    	hs.add("!<");
    	hs.add("<");
    	hs.add("!=");
    	hs.add("=");
    	hs.add(" like ");
    	hs.add(" Like ");
    	hs.add(" LIKE ");
        Iterator itr = db.rf.tableAttributes.entrySet().iterator();
        
        countForNumOfTables = 0;
        
        
        //Printing hashmap
    /*	while(itr.hasNext())
        {
            Map.Entry<String , LinkedList<String>> mEntry = (Map.Entry<String , LinkedList<String>>) itr.next();
            String key = (String) mEntry.getKey();
            ll = new LinkedList<String>();
            ll =  mEntry.getValue();
            Iterator x;
            x = ll.listIterator();
            System.out.println("Table Name:"+key);
            while(x.hasNext())
            {
               String st=(String)x.next();
               System.out.println("string="+st);
            }
        }
*/
    	/*query = "SELECT NAME " +
                "FROM   countries As t1 , Identificatioan_newwwd As t2 " +
                "WHERE  'ss'=t1.NAME ANd 72=t1.ID AND t1.NAME=t2.NAME  "+
                "GROUP BY t1.CODE , t1.ID " + 
                "Having t1.NAME='jkj' AND t1.CODE = 'abc' " +
                "ORDER BY t1.CODE ;" ;
    	*/
    	
        sqlparser = new TGSqlParser(EDbVendor.dbvoracle);

        sqlparser.sqltext = query;
             
        int ret = sqlparser.parse();
        if (ret == 0)
        {
            for(int i=0;i<sqlparser.sqlstatements.size();i++)
            {
                analyzeStmt(sqlparser.sqlstatements.get(i));
              //  System.out.println("");
            }
        }
        else
        {
        	invalidQueryFlag = true;
        	System.out.println("Query Invalid");
         //   System.out.println(sqlparser.getErrormessage());
        }

    }

    protected static void analyzeStmt(TCustomSqlStatement stmt){

        switch(stmt.sqlstatementtype){
            case sstselect:
                analyzeSelectStmt((TSelectSqlStatement)stmt);
                break;
            case sstupdate:
                break;
            case sstcreatetable:
            	
                break;
            case sstaltertable:
                break;
            case sstcreateview:
                break;
            default:
                System.out.println(stmt.sqlstatementtype.toString());
        }
    }
    
    public static boolean indexExist(String tbName)
    {
    	String indexFile="";
    	indexFile = "index_"+tbName;
    	
    	boolean returnValue=false;
    	try
    	{
    		System.out.println("3");
    		FileReader fstream = new FileReader("/home/salman/Videos/"+indexFile);
			
			BufferedReader out = new BufferedReader(fstream);		
    		returnValue =true;
    	}
    	catch (Exception e)
    	{
    		returnValue = false;
    		System.out.println("4");
    	//	e.printStackTrace();
    	}
    	return returnValue;
    }
    
    
    static TreeMap<String,String> indexHashMap = new TreeMap<String,String>();
	private static boolean invalidqueryQueryFlag;
    public static void insertIntoIndexHashMap(String columnName,String columndata,long offset)
    {
    	if(indexHashMap.containsKey(columndata))
    	{
    		String val = indexHashMap.get(columndata);
    		val = val +","+ String.valueOf(offset);
    		indexHashMap.put(columndata, val);
    	}
    	else
    	{
    		indexHashMap.put(columndata,String.valueOf(offset));
    	}
    	
    	
    }
    
    public static void CreateIndexFile(String tbName,String result)
    {
    	String line="",dataLine="",token[];
    	
    	long offset=0,len;
    	int i;
    	try
    	{
    		FileWriter fstream1 = new FileWriter("/home/salman/Videos/index_"+tbName);
			
			BufferedWriter out1 = new BufferedWriter(fstream1);							 
			
    		
			System.out.println("5");
    		RandomAccessFile tableData = new RandomAccessFile("/home/salman/Videos/"+tbName+".csv", "rw");
    		System.out.println("6");
    		RandomAccessFile file = new RandomAccessFile("/home/salman/Videos/"+tbName+".data", "rw");
    		System.out.println("7");
    		line=file.readLine();
    		String Attributes [] = line.split(",");
    		for(i=0 ; i<Attributes.length ; i++)
    		{
    			String attributeNames[] = Attributes[i].split(":");
    			len=0;
    			while((dataLine=tableData.readLine())!=null)
    			{
    				
    				token=dataLine.split(",");
    				insertIntoIndexHashMap(attributeNames[0], token[i], len);
    				len += dataLine.length();
    			}
    			FileWriter fstream = new FileWriter("/home/salman/Videos/index_"+tbName+"_"+attributeNames[0]);
    			
				BufferedWriter out = new BufferedWriter(fstream);							 
				
    			Iterator iter = indexHashMap.entrySet().iterator();
    			while (iter.hasNext()) 
				{
					Map.Entry mEntry = (Map.Entry) iter.next();
			
					out.write((String)mEntry.getKey()+":"+(String)mEntry.getValue());
				
					
					out.write("\n");
				}
				out.close();
				indexHashMap.clear();
				
    		}
    	}
    	catch(Exception e)
    	{
    		System.out.println("8");
    		e.printStackTrace();
    	} 	
    }
    
    public static boolean whereAndHavingValidator(String str)
    {
    	//System.out.println("\nstr====="+str);
    	
    	boolean flag=false;
    	//System.out.println("string="+str)
    	if(str.toLowerCase().contains(" or "))
        {
        	String tokenOr[] = str.split("(?i)( or )");
        	
        	for(int i=0; i<tokenOr.length; i++)
        	{
        		
        		if(tokenOr[i].toLowerCase().contains(" and "))
        		{
        			String tokenAnd[] = tokenOr[i].split("(?i)( and )");
        		//	System.out.println("string1="+tokenAnd[i]);
        			for(int j=0 ; j<tokenAnd.length ; j++)
        			{
        				//System.out.println("str="+tokenAnd[j]);
        				flag = validator(tokenAnd[j]);
        				if(flag == false)
        				{	
        					//System.out.println("--1");
        					break;
        				}
        			}
        			
        			if(flag == false)
        				break;
        		}
        		else
        		{
        			flag=validator(tokenOr[i]);
        			if(flag==false)
        			{	
        				//System.out.println("--2");
        				break;
        			
        			}
        		}                		
        	}
        }
        else if(str.toLowerCase().contains(" and "))
        {
        	String tokenAnd[] = str.split("(?i)( and )");
			for(int j=0 ; j<tokenAnd.length ; j++)
			{
				flag = validator(tokenAnd[j]);
				if(flag == false)
				{		
					break;
				}
			}
        }
        else
        {
 //       	System.out.println("str="+str);
        	flag=validator(str);
        }
        
    	return flag; 	
    }
    
    
    
    
    protected static void analyzeSelectStmt(TSelectSqlStatement pStmt){
    	
    	String result="";
    	
    	
        //System.out.println("Querytype:select");
    	
    	result="Querytype:select\n";
    	
        if (pStmt.isCombinedQuery())
        {
            String setstr="";
            switch (pStmt.getSetOperator())
            {
                case 1: setstr = "union";break;
                case 2: setstr = "union all";break;
                case 3: setstr = "intersect";break;
                case 4: setstr = "intersect all";break;
                case 5: setstr = "minus";break;
                case 6: setstr = "minus all";break;
                case 7: setstr = "except";break;
                case 8: setstr = "except all";break;
            }
            System.out.printf("set type: %s\n",setstr);
            System.out.println("left select:");
            analyzeSelectStmt(pStmt.getLeftStmt());
            System.out.println("right select:");
            analyzeSelectStmt(pStmt.getRightStmt());
            if (pStmt.getOrderbyClause() != null)
            {
                System.out.printf("order by clause %s\n",pStmt.getOrderbyClause().toString());
            }
        }
        else
        {
            //select list
            

            //from clause, check this document for detailed information
            //http://www.sqlparser.com/sql-parser-query-join-table.php
        	//System.out.print("Tablename:");
        	
        	result += "Tablename:";
        	
        	int count=0;
        	String Tablename="";
            for(int i=0;i<pStmt.joins.size();i++)
            {
                TJoin join = pStmt.joins.getJoin(i);
              //  System.out.println("kind="+join.getKind());
                switch (join.getKind())
                {
                    case TBaseType.join_source_fake:
                    	Tablename=join.getTable().toString();
                    	//System.out.println("Tablenamesdsad:"+Tablename);
                    	tableName.add(Tablename+".csv");
                    	result += (Tablename+",");
                    	tbName = Tablename ;
                    	tableNamesInTheQuery[countForNumOfTables++] = Tablename;
                        //System.out.printf("%s Alias as %s:",Tablename,(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"");
                    	if(join.getTable().getAliasClause()!=null)
                    	{
                    		//System.out.println(join.getTable().getAliasClause()+"===");
                    		aliasingHash.put(join.getTable().getAliasClause().toString(),Tablename );
                    		reverseAliasingHash.put(Tablename, join.getTable().getAliasClause().toString());
                    	}
                        if(!db.rf.tableAttributes.containsKey(Tablename+".csv"))
                        {
                        	//System.out.println("Invalid Query tablename mei");
                        	//System.exit(0);
                        	invalidQueryFlag = true;
                        }
                        if(count!=pStmt.joins.size()-1)
                        {
                        	//System.out.print(",");
                        	
                        }
                        count++;
                        break;
                    case TBaseType.join_source_table:
                    	
                    	Tablename=join.getTable().toString();
                    	System.out.println("Tablenamesdsad:"+Tablename);
                    	tableName.add(Tablename+".csv");
                    	result += (Tablename+",");
                    	tbName = Tablename ;
                    	tableNamesInTheQuery[countForNumOfTables++] = Tablename;
                    	
                    	
                    	table1 = join.getTable().toString();
                    	tableNamesInJoin[tableCounter++] = table1;
                     	flagForCheckingJoin = true;
                    	if(join.getTable().toString()!=null)
                    	{
                    		System.out.println(join.getTable().getAliasClause()+"===");
                    		aliasingHash.put(join.getTable().toString(),Tablename );
                    		reverseAliasingHash.put(Tablename, join.getTable().toString());
                    	}
                        System.out.printf("\nbtable: \n\t%s, alias: %s\n",join.getTable().toString(),(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"");
                        for(int j=0;j<join.getJoinItems().size();j++)
                        {
                        	
                        	
                        	
                            TJoinItem joinItem = join.getJoinItems().getJoinItem(j);
                            System.out.printf("Join type: %s\n",joinItem.getJoinType().toString());
                            
                            

                        	Tablename=joinItem.getTable().toString();
                        	//System.out.println("Tablenamesdsad:"+Tablename);
                        	tableName.add(Tablename+".csv");
                        	result += (Tablename+",");
                        	tbName = Tablename ;
                        	tableNamesInTheQuery[countForNumOfTables++] = Tablename;
                            //System.out.printf("%s Alias as %s:",Tablename,(join.getTable().getAliasClause() !=null)?join.getTable().getAliasClause().toString():"");
                        	if(joinItem.getTable().toString()!=null)
                        	{
                        		//System.out.println(join.getTable().getAliasClause()+"===");
                        		aliasingHash.put(joinItem.getTable().toString(),Tablename );
                        		reverseAliasingHash.put(Tablename, joinItem.getTable().toString());
                        	}
//                            if(!db.rf.tableAttributes.containsKey(Tablename+".csv"))
//                            {
//                            	//System.out.println("Invalid Query tablename mei");
//                            	//System.exit(0);
//                            	invalidQueryFlag = true;
//                            }
                            
                            
                            
                            
                            
                            
                            
                            table2 = joinItem.getTable().toString();
                            tableNamesInJoin[tableCounter++] = table2;
                            
                            System.out.printf("ctable: %s, alias: %s\n",joinItem.getTable().toString(),(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"");
                            if (joinItem.getOnCondition() != null)
                            {
                            	joiningCondition = joinItem.getOnCondition().toString();
                                System.out.printf("On: %s\n",joinItem.getOnCondition().toString());
                            }
                            else  if (joinItem.getUsingColumns() != null)
                            {
                                System.out.printf("using: %s\n",joinItem.getUsingColumns().toString());
                            }
                        }
                        
                        if(tableCounter > 2)
                        {
                        	PartC obj = new PartC(configFile);
                        	obj.CostEstimation(tableNamesInJoin,tableCounter);
                        	return;
                        }
                       
                        break;
                    case TBaseType.join_source_join:
                        TJoin source_join = join.getJoin();
                        System.out.printf("\ndtable: \n\t%s, alias: %s\n",source_join.getTable().toString(),(source_join.getTable().getAliasClause() !=null)?source_join.getTable().getAliasClause().toString():"");

                        for(int j=0;j<source_join.getJoinItems().size();j++)
                        {
                            TJoinItem joinItem = source_join.getJoinItems().getJoinItem(j);
                            System.out.printf("source_join type: %s\n",joinItem.getJoinType().toString());
                            System.out.printf("table: %s, alias: %s\n",joinItem.getTable().toString(),(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"");
                            if (joinItem.getOnCondition() != null)
                            {
                                System.out.printf("On: %s\n",joinItem.getOnCondition().toString());
                            }
                            else  if (joinItem.getUsingColumns() != null)
                            {
                                System.out.printf("using: %s\n",joinItem.getUsingColumns().toString());
                            }
                        }

                        for(int j=0;j<join.getJoinItems().size();j++)
                        {
                            TJoinItem joinItem = join.getJoinItems().getJoinItem(j);
                            System.out.printf("Join type: %s\n",joinItem.getJoinType().toString());
                            System.out.printf("etable: %s, alias: %s\n",joinItem.getTable().toString(),(joinItem.getTable().getAliasClause() !=null)?joinItem.getTable().getAliasClause().toString():"");
                            if (joinItem.getOnCondition() != null)
                            {
                                System.out.printf("On: %s\n",joinItem.getOnCondition().toString());
                            }
                            else  if (joinItem.getUsingColumns() != null)
                            {
                                System.out.printf("using: %s\n",joinItem.getUsingColumns().toString());
                            }
                        }

                        break;
                    default:
                        System.out.println("unknown type in join!");
                        
                }                
            }
            
            
            result = result.substring(0, result.length()-1);
            
            result += "\nColumns:";
            
            String columnString="";
            
            //column
            
            JoiningTables jnt = new JoiningTables();
            
             if(sqlparser.sqltext.contains("*"))
             {
            	 String returnValue="";
            	if(flagForCheckingJoin == false)
            	{	
            		returnValue  = printTheColumnsForStarQuery();
            	}
            	else
            	{
            		jnt.joiningResult(table1,table2,joiningCondition,"*",db.rf.pathForData,configFile);
            		return;
            	}
            	//System.out.println("*:"+returnValue);
            	
            	result += returnValue;
            	
             }
             else
             {	 
            	 //joiningResult(table1,table2,joiningCondition); 
	       	    
            	String colsForJoin="";
	       	    for(int k=0; k < pStmt.getResultColumnList().size();k++)
	            {
	       	    	int number=1;
	                TResultColumn resultColumn = pStmt.getResultColumnList().getResultColumn(k);
	                String str=resultColumn.getExpr().toString();
	          //      System.out.println("str="+str);
	                int first,second;
	                if(str.contains("("))
	                {
	                	boolean returnValue=false;
	                	
	                	first=str.lastIndexOf('(');
	                	
	                	second = str.indexOf(')');
	                	String columnName=str.substring(first+1, second);
	                	if(str.contains("."))
	                	{
	                		returnValue = checkForColumnNameInAliasing(columnName.trim());
	                	}
	                	else
	                	{
	                		returnValue = checkForColumnName(columnName.trim());
	                	}
	                	if(returnValue==true)
	                	{
	              //  		System.out.printf("%s",columnName);
	                		if(!columnName.contains("."))
	                		{
	                			number=getNumberOfCountsOfTheTablesInWhichColumnExist(columnName);
	                		}
	                		columnString= columnString + columnName;
	                	}
	                	else
	                	{
	                		//System.out.println("Invalid Query1c");
	                //		System.exit(0);
	                		invalidQueryFlag = true;
	                	}
	                	if(number>1)
	                		invalidQueryFlag=true;
	                }
	                else
	                {
	                	//System.out.println("col="+str);
	                	boolean returnValue=false;
	                	if(str.contains("."))
	                	{
	                		System.out.println("str:"+str);
	                		returnValue = checkForColumnNameInAliasing(str.trim());
	                	}
	                	else
	                	{
	                		returnValue = checkForColumnName(str.trim());
	                	}
	                	if(returnValue==true)
	                	{	//System.out.printf("%s",str);
	                		if(!str.contains("."))
	                		{
	                			number=getNumberOfCountsOfTheTablesInWhichColumnExist(str);
	                		}
	                		columnString= columnString +str.trim();
	                	}
	                	else
	                	{
	                		//System.out.println("Invalid Query2c");
	                //		System.exit(0);
	                		invalidQueryFlag = true;
	                		continue;
	                	}
	                	if(number>1)
	                	{	
	                		invalidQueryFlag=true;
	                		continue;
	                	}
	                //	System.out.printf("%s",str);
	                }
	                if(k!=pStmt.getResultColumnList().size()-1)
	                {
	                //	System.out.print(",");
	                	columnString = columnString + ",";
	                }
	              
	            }
	            //System.out.println(columnString);
	            
             }
            
             columnValueToDbSystem = columnString;
             System.out.println("COLS:" + columnString);
             
             if(flagForCheckingJoin == true)
             {
            	 jnt.joiningResult(table1,table2,joiningCondition,columnValueToDbSystem,db.rf.pathForData,configFile);
            	 return;
             }
             System.out.println("column:" + columnValueToDbSystem);
             result += columnString;
             
             result += "\nDistinct:";
             
             String str_distinct="";

            
             //Distinct
             
             
            if(sqlparser.sqltext.toLowerCase().contains("distinct"))
            {
            	int i,j;
            	
            	String tok[] = sqlparser.sqltext.split("(?i)(distinct)");
            	//System.out.print("\nDistinct:");
            	if(tok.length > 2 )
            	{
            		invalidQueryFlag = true;
            	}
            	for(i=0;i<tok.length;i++)
            	{
            //		System.out.println("tok="+tok[i]);
            		if(tok[i].toLowerCase().contains("select"))
            			continue;
            		if(tok[i].toLowerCase().contains("from"))
            		{
            			for(j=0;j<tok[i].length();j++)
            			{
            				if(tok[i].charAt(j)!=' ')
            					break;
            			}
            			String str="";
            			for(;j<tok[i].length();j++)
            			{
            				if(tok[i].charAt(j)==' ')
            					break;
            				str = str+tok[i].charAt(j);
            			}
            			int first,second;
            			first=str.lastIndexOf('(');
                    	second =str.indexOf(')');
            			str_distinct = str_distinct + str.substring(first+1,second);
            		}
            		else
            		{
            			int first,second;
            			first=tok[i].indexOf('(');
                    	second = tok[i].indexOf(')');
                    	//System.out.println("first="+first+" second="+second);
                   // 	System.out.printf("%s",str.substring(first+1, second));
                    	
            			str_distinct = str_distinct + tok[i].substring(first+1,second)+",";
            		}
            	}
            	//System.out.println(str_distinct);            	
            }
            else
            {
            	str_distinct = "NA";
            	//System.out.println("\nDistinct:NA");
            }
            
            result += (str_distinct+"\n");
            
            
            result += "Condition:"; 
            
            //where clause
            
            
            String strWhere = "";
            
            if (pStmt.getWhereClause() != null)
            {
            	String strWhereClause=pStmt.getWhereClause().getCondition().toString();
            	
            	strWhere = strWhereClause;
            	//System.out.println("value="+strWhereClause);
            	
               // System.out.printf("\nCondition:%s", strWhereClause);
                boolean flag = false;
                
                flag = whereAndHavingValidator(strWhereClause);
                
                if(flag == false)
                {
                	//System.out.println("\nInvalid Query w");
                	invalidQueryFlag = true;
                }
                
            }
            else
            {
            	//System.out.println("\nCondition:NA");
            	strWhere = "NA";
            }

            if(strWhere.compareToIgnoreCase("NA")!=0)
            	whereClauseValueToDbSystem = strWhere;
            else
            	whereClauseValueToDbSystem="";
            result += (strWhere+"\n");
            
            result += "Orderby:";
            
            String orderByName="";
            
            
            // order by
            if (pStmt.getOrderbyClause() != null)
            {
            	//System.out.printf("\nOrderby:");
            	int orderBySize=pStmt.getOrderbyClause().getItems().size();
            	int number=1;
            	boolean flag=false;
                for(int i=0;i<orderBySize;i++)
                {
                	String columnName_OrderBy = pStmt.getOrderbyClause().getItems().getOrderByItem(i).toString();
                	if(!columnName_OrderBy.contains("."))
            		{
            			number=getNumberOfCountsOfTheTablesInWhichColumnExist(columnName_OrderBy);
            		}
                	if(number>1)
                		invalidQueryFlag=true;
            		
                	orderByName = orderByName + columnName_OrderBy;
                //    System.out.printf("%s", columnName_OrderBy);
                	if(columnName_OrderBy.contains("."))
                	{
                		flag=checkForColumnNameInAliasing(columnName_OrderBy.trim());
                	}
                	else
                	{
                		flag=checkForColumnName(columnName_OrderBy.trim());
                	}
                	if(flag==false)
                	{
                		//System.out.println("Invalid query orderby");                		
                //		exit(0);
                		//break;
                		invalidQueryFlag = true;
                		
                	}
                    if(i!=orderBySize-1)
                    {
                    	//System.out.print(",");
                    	orderByName = orderByName + ",";
                    }
                }
            //    System.out.println(orderByName);
            }
            else
            {
            	//System.out.println("\nOrderby:NA");
            	
            	orderByName = "NA";
            }
            
            
            result += (orderByName+"\n");
            orderByClauseValueToDbSystem = orderByName;
            
            result += "Groupby:";
            String groupByName="";
            String strHaving="";
            
          //  pStmt.getGroupByClause()
            // group by
            
            int f = 0;
            
            if (pStmt.getGroupByClause() != null)
            {
            	f = 1;
            	//System.out.print("\nGroupby:");
            	int number=1;
            	boolean flag=false;
            	int groupBySize=pStmt.getGroupByClause().getItems().size();
            	for(int i=0;i<pStmt.getGroupByClause().getItems().size();i++)
                {
            		String columnName_GroupBy=pStmt.getGroupByClause().getItems().getGroupByItem(i).toString();
            		
            		if(!columnName_GroupBy.contains("."))
            		{
            			number=getNumberOfCountsOfTheTablesInWhichColumnExist(columnName_GroupBy);
            		}
                	if(number>1)
                		invalidQueryFlag=true;
            		
            		
            		groupByName = groupByName + columnName_GroupBy; 
            		//System.out.printf("%s",columnName_GroupBy);
            		if(columnName_GroupBy.contains("."))
            		{
            			flag=checkForColumnNameInAliasing(columnName_GroupBy.trim());
            		}
            		else
            		{
            			flag=checkForColumnName(columnName_GroupBy.trim());
            		}
            		if(flag==false)
                	{
                		//System.out.println("Invalid query group by");                		
                //		exit(0);
                		//break;
            			invalidQueryFlag = true;
            			
                	}
            		if(i!=groupBySize-1)
            		{
            	//		System.out.print(",");
            			groupByName = groupByName + ",";
            		}
                }
            	
            	//result += (groupByName+"\n");
            	
            	//strHaving = "Having:";
            	
            	//System.out.println(groupByName);
            	if(pStmt.getGroupByClause().getHavingClause()!=null)
            	{
            		//System.out.print("\nHaving:");
            		
            		
            		String strHavingClause = pStmt.getGroupByClause().getHavingClause().toString();
            		
            		strHaving = strHavingClause;
            		
            		flag = whereAndHavingValidator(strHavingClause);

            		if(flag == false)
            		{
            			//System.out.println("Invalid Query having wali");
            			//exit(0);
            			invalidQueryFlag = true;
            		}
            		
            		//System.out.println(strHavingClause);
            	}
            	else
            	{
            		//System.out.println("\nHaving:NA");
            		strHaving = "NA";
            	}
            	
            	
            }
            else
            {
            	//System.out.println("\nGroupby:NA\nHaving:NA");
            	groupByName = "NA";
            	
            }   
            
            result += (groupByName);
            
            
            if( f == 1 )
            {
            	result += "\n"+"Having:";
            	result += (strHaving);
            }
            else
            {
            	result += "\n"+"Having:";
            	result += "NA";
            }
            if(invalidqueryQueryFlag)
            	System.out.println("Query Invalid");
            else
            {
            	valueToDbSystem = 1;
            //	boolean indexFileExist = indexExist(tbName);
            //	System.out.println(indexFileExist);
            //	if(indexFileExist == false)
           // 	{
           // 		CreateIndexFile(tbName,result);
            //	}
            	
            	//System.out.println("hello");
            //	System.out.println(result);
            
            }
       /*     if(sqlparser.sqltext.toLowerCase().contains("having"))
            {
            	int i,offset_having,offset_orderby;
            	offset_having=sqlparser.sqltext.toLowerCase().indexOf("having");
            	offset_orderby=sqlparser.sqltext.toLowerCase().indexOf("order");
            	if(offset_orderby==-1)
            		System.out.println("\nHaving:"+sqlparser.sqltext.substring(offset_having+7,sqlparser.sqltext.length()-1));
            	else
            		System.out.println("\nHaving:"+sqlparser.sqltext.substring(offset_having+7,offset_orderby));
            	
            }
            else
            {
            	System.out.println("\nHaving:NA");
            }
         */   
            
            // for update
          /*  if (pStmt.getForUpdateClause() != null){
                System.out.printf("for update: \n%s\n",pStmt.getForUpdateClause().toString());
            }

            // top clause
            if (pStmt.getTopClause() != null){
                System.out.printf("top clause: \n%s\n",pStmt.getTopClause().toString());
            }

            // limit clause
            if (pStmt.getLimitClause() != null){
                System.out.printf("top clause: \n%s\n",pStmt.getLimitClause().toString());
            }*/
        }
    }
}