package com.db.main;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;

import test.hive.tutorial;

public class Selectquery {
	static String strWhereClause="";
	static String columnClause="";
	static String tableName="";
	static String strRecord="";
	static String condition1="and";
	static String condition2="or";
	static long lastRecordId=0;
	static String expression="";
	static int arr[];
	static boolean flag=false;
	static LinkedHashSet <String> hs = new LinkedHashSet<String>();
	static DBSystem dbsystem = new DBSystem();
	static RandomAccessFile dumpFile,dumpFileInDuplicate;
	static FileWriter fstream1  , fstream2,fstream3;
	static BufferedWriter out1,out2,out3;
	static String orderByColumns[];
	static int orderByColumnsPosition[];
	static boolean flagForExistenceOfOrderByClause=false;
	static int numOfOrderByClauseElements=0; 
	static String columnNames[]=new String[100];
	static String columnDatatype[]=new String[100];
	
	
	static int lengthForReturnRecord=0;
	static int numberForReturnRecord=0;
	static String tokenForReturnRecord[];
	
	public static String returnRecord(int i)
	{
		//System.out.println("hellosallu");
		
		
		
		if(i >= lengthForReturnRecord)
		{
		
			tokenForReturnRecord = dbsystem.getAllrecord(tableName, i).split("\n");
			numberForReturnRecord = dbsystem.returnFirstID(i, tableName+".csv");
	//		System.out.println("firstid="+numberForReturnRecord);
			lengthForReturnRecord += tokenForReturnRecord.length;
			System.out.println("returnRecord="+tokenForReturnRecord[i-numberForReturnRecord]);
			return tokenForReturnRecord[i-numberForReturnRecord];
			
		}
		else		
		{
			System.out.println("returnRecord="+tokenForReturnRecord[i-numberForReturnRecord]);
		//	System.out.println("i="+i+"\tnumber="+numberForReturnRecord);
			return tokenForReturnRecord[i-numberForReturnRecord];
		}
	}
	public static boolean recordCheckForString(String operator,String expressionValue,String recordValue)
	{
		if(expressionValue.charAt(0)=='"' && expressionValue.charAt(expressionValue.length()-1)=='"')
			expressionValue = expressionValue.substring(1, expressionValue.length()-1);
		switch(operator)
		{
			case	"<>" : 		if(recordValue.compareTo(expressionValue)!=0)
									return true;
								else
									return false;
			case  ">="	: 		if(recordValue.compareTo(expressionValue) >= 0)
									return true;
								else
									return false;
			case  "!>"	: 		if(recordValue.compareTo(expressionValue) <= 0)
									return true;
								else
									return false;
			case  ">"	: 		if(recordValue.compareTo(expressionValue) > 0)
									return true;
								else
									return false;
			case  "<="	: 		if(recordValue.compareTo(expressionValue) <= 0)
									return true;
								else
									return false;
			case  "!<"	: 		if(recordValue.compareTo(expressionValue) >= 0)
									return true;
								else
									return false;
			case  "<"	: 		if(recordValue.compareTo(expressionValue) < 0)
									return true;
								else
									return false;
			case  "!="	: 		if(recordValue.compareTo(expressionValue) != 0)
									return true;
								else
									return false;
			case  "="	: 		if( recordValue.compareTo(expressionValue) == 0)
									return true;
								else
									return false;
			case "like" :		if( recordValue.compareToIgnoreCase(expressionValue) == 0)
									return true;
								else
									return false;
			case "Like" :		if( recordValue.compareToIgnoreCase(expressionValue) == 0)
									return true;
								else
									return false;
			case "LIKE" :		if( recordValue.compareToIgnoreCase(expressionValue) == 0)
									return true;
								else
									return false;
		}
		return false;
	}
	public static boolean recordCheckForFloat(String operator , double expressionValue , double recordValue)
	{
		switch(operator)
		{
			case	"<>" : 		if(recordValue  != expressionValue)
									return true;
								else
									return false;
			case  ">="	: 		if(recordValue >= expressionValue)
									return true;
								else
									return false;
			case  "!>"	: 		if( recordValue <=  expressionValue)
									return true;
								else
									return false;
			case  ">"	: 		if(  recordValue  > expressionValue )
									return true;
								else
									return false;
			case  "<="	: 		if( recordValue <= expressionValue )
									return true;
								else
									return false;
			case  "!<"	: 		if( recordValue  >= expressionValue )
									return true;
								else
									return false;
			case  "<"	: 		if(recordValue  < expressionValue )
									return true;
								else
									return false;
			case  "!="	: 		if( recordValue  != expressionValue )
									return true;
								else
									return false;
			case  "="	: 		if( recordValue == expressionValue)
									return true;
								else
									return false;

		}
		return false;
	}
	
	
	public static boolean recordCheckForInt(String operator , long expressionValue , long recordValue)
	{
		switch(operator)
		{
			case	"<>" : 		if(recordValue  != expressionValue)
									return true;
								else
									return false;
			case  ">="	: 		if(recordValue >= expressionValue)
									return true;
								else
									return false;
			case  "!>"	: 		if( recordValue <=  expressionValue)
									return true;
								else
									return false;
			case  ">"	: 		if(  recordValue  > expressionValue )
									return true;
								else
									return false;
			case  "<="	: 		if( recordValue <= expressionValue )
									return true;
								else
									return false;
			case  "!<"	: 		if( recordValue  >= expressionValue )
									return true;
								else
									return false;
			case  "<"	: 		if(recordValue  < expressionValue )
									return true;
								else
									return false;
			case  "!="	: 		if( recordValue  != expressionValue )
									return true;
								else
									return false;
			case  "="	: 		if( recordValue == expressionValue)
									return true;
								else
									return false;

		}
		return false;
		
	}
	
	public static boolean checkRecord( String recordValue , String datatype , String expressionValue , String operator  )
	{
		 long recordInInt=0;
		 double recordInDouble=0.0;
		 long expressionInInt=0;
		 double expressionInDouble=0.0;
		 int flag=0;
		 
		 boolean returnValue=false;
		 switch(datatype.toLowerCase())
		 {
		 	case "integer" :	recordInInt = Integer.parseInt(recordValue);
		 						expressionInInt = Integer.parseInt(expressionValue);
		 						flag=1;
		 						break;
		 	case "float" 	: 	recordInDouble = Double.parseDouble(recordValue);
		 						expressionInDouble = Double.parseDouble(expressionValue);
		 						flag=2;
		 						break;
		 }
		 switch(flag)
		 {
		 	case 0 : 			returnValue = recordCheckForString(operator, expressionValue, recordValue);
		 						return returnValue;
		 	
		 	case 1 :			returnValue = recordCheckForInt(operator, expressionInInt, recordInInt);
		 	 					return returnValue;
		 	case 2 :			returnValue = recordCheckForFloat(operator, expressionInDouble, recordInDouble);
		 	 					return returnValue;
		 	 
		 }
		 return false;
	}
	static int nui=0;
	public static String sort(String data)
	{
		
		if(nui==0){
//		System.out.println("data="+data+"----------");
		String records[] = data.split("\n");
	//	System.out.println("data="+data );
		
	//F	System.out.println("------------------------");
		String min = "";
		String returnValue = "";
		int minPos = 0;
		int length1=0;
	//	System.out.println("length="+records.length);
		String records1[] = new String[records.length];
		int len=0;
//		System.out.println("-----");
		for(int i=0;i<records.length;i++)
		{
		//	System.out.println("r1="+records[i]);
			if(records[i].length() > 1)
				length1++;
			if(records[i].length() > 1 && records[i].charAt(0)==',')
				records1[len++]= records[i].substring(1,records[i].length());
			else if	(records[i].length() > 1)
				records1[len++]= records[i].substring(0,records[i].length());
		
		}
		
	//	for(int i=0;i<length1;i++)
	//		System.out.println("r2="+records1[i]);
		
	//	System.out.println("-----");
		int i=0;
		
		for(  ;i<length1  ; 	i++ )
		{
//			System.out.println("length="+records.length);
//			System.out.println("i="+i);
			//if()
			//	break;
			min = "";
			minPos = 0;
	//		min=records1[i];
	//		minPos=i;
			
	//		System.out.println("rec="+records1[i]);
		//	if(records1[i].compareToIgnoreCase("")==0)
		//		continue;
			
//			if(records1[i].length() >= 1 && records[i].charAt(0)==',')
	//			records[i]= records[i].substring(1,records[i].length());
			//System.out.println("1="+records[i]);
			
			for( int j = 0 ; j < records.length ; j++ )
			{
				
				
			//	System.out.println("rec2="+records1[j]);
				
				
				if(records1[j].compareToIgnoreCase("")==0)
					continue;
			
				if( min.compareToIgnoreCase("") == 0 )
				{
						min = records1[j];
						minPos = j;
//						if(records[j].length()==1)
	//						continue;
						
			//			if(records1[j].compareToIgnoreCase("")==0)
			//				continue;
						
					}
					else
					{
		//				if(records[j].length()==1)
			//				continue;
			//			if(records[j].compareToIgnoreCase("")==0)
			//				continue;
						
				//		System.out.println("2="+min);
						for( int k = 0 ; k < orderByColumns.length ; k++ )
						{
							int l = orderByColumnsPosition[k];
							
							String column = orderByColumns[k];
							
							String dataType = columnDatatype[l];
							
							String minSplit[] = min.split(",");
							
						//	System.out.println("record="+records[j]);
					//		if(records[j].charAt(0)==',')
					//			records[j] = records[j].substring(1,records[j].length());
							String recordSplit[] = records1[j].split(",");
							
				//			if(recordSplit.length==1)
					//			break;
							
					//		System.out.println(recordSplit.length);
							
							String str1 = minSplit[l];
							
							String str2 = recordSplit[l];
							
					//		System.out.println("str1="+str1);
					//		System.out.println("str2="+str2);
							
							if( str1.charAt(0) == '"' && str1.charAt(str1.length()-1) == '"' )
								str1 = str1.substring(1,str1.length()-1);
							
							if( str2.charAt(0) == '"' && str2.charAt(str2.length()-1) == '"' )
								str2 = str2.substring(1,str2.length()-1);
						
							int flag = 0;
							
							switch(dataType)
							{
								case "integer" : 	if( Integer.parseInt(str1) > Integer.parseInt(str2) )
													{
														min = records1[j];
														minPos = j;
														flag = 1;
													}
													else if( Integer.parseInt(str1) < Integer.parseInt(str2) )
													{
														flag = 1;
													}	
													else
													{
														flag = 0;
													}
														
													break;
								case "float"   :	if( Double.parseDouble(str1) > Double.parseDouble(str2) )
													{
														min = records1[j];
														minPos = j;
														flag = 1;
													}
													else if( Double.parseDouble(str1) < Double.parseDouble(str2) )
													{
														flag = 1;
													}
													else
													{
														flag = 0;
													}
													break;
								default			:	if( str1.compareToIgnoreCase(str2) > 0 )
													{
														min = records1[j];
														minPos = j;
														flag = 1;
													}
													else if( str1.compareToIgnoreCase(str2) < 0 )
													{
														flag = 1;
													}
													else
													{
														
														flag = 0;
													}
													break;
							}
							
							if( flag == 1 )
								break;
							
							
						}
					}
			}
		//	System.out.println("min="+min);
		//	System.out.println("minpos[j]="+records1[minPos]);
			returnValue += ( min + "\n" );
			
			records1[minPos] = "";
		
			
		}
//		System.out.println("ret="+returnValue);
//		System.out.println("---------------------------");
		return returnValue;
		}
		else
		{
			String abc[]=data.split("\n");
			return abc[0];
		}
	}
	
	public static void merge(String PathForData)
	{
		int count=0;
		try
		{
			
			out1.close();
			out2.close();
			RandomAccessFile file1 = new RandomAccessFile(PathForData+"f1.txt", "r");
			RandomAccessFile file2 = new RandomAccessFile(PathForData+"f2.txt", "r");
			String strOut1 = "" , strOut2 = "";
			String finalValueToBePlacedInOut3="";
			strOut1 = file1.readLine();
			strOut2 = file2.readLine();
			while(true)
			{
				if(strOut1 == null || strOut2 == null)
					break;
				String sortInFile=sort(strOut1+"\n"+strOut2);
				String min[] = sortInFile.split("\n");
				//out3.write(min[0]+"\n");
				finalValueToBePlacedInOut3 = finalValueToBePlacedInOut3 + min[0] + "\n";
				if(min[0].compareToIgnoreCase(strOut1)==0)
					strOut1 = file1.readLine();
				else
					strOut2 = file2.readLine();
				count++;
			}
			
			while(true)
			{
				if(strOut1 == null)
					break;
				finalValueToBePlacedInOut3 = finalValueToBePlacedInOut3 + strOut1 + "\n";
				strOut1 = file1.readLine();
				count++;
			}
			while(true)
			{

				if(strOut2 == null)
					break;
				
				finalValueToBePlacedInOut3 = finalValueToBePlacedInOut3 + strOut2+"\n";
				strOut2 = file2.readLine();
				count++;
			}
			System.out.println("count="+count);
			out3.write(finalValueToBePlacedInOut3.substring(0, finalValueToBePlacedInOut3.length()-1));
			out3.close();
		//	System.out.println("final="+finalValueToBePlacedInOut3);
			//RandomAccessFile file3 = new RandomAccessFile("/home/salman/Videos/f3", "rw");
			fstream1 = new FileWriter(PathForData+"f1.txt",false);
			
			out1 = new BufferedWriter(fstream1);	
			
			fstream2 = new FileWriter(PathForData+"f2.txt",false);
			
			out2 = new BufferedWriter(fstream2);
			
			fstream3 = new FileWriter(PathForData+"f3.txt",false);
			
			out3 = new BufferedWriter(fstream3);	
			
			//System.out.println("final="+finalValueToBePlacedInOut3);
			
			out1.write(finalValueToBePlacedInOut3.substring(0, finalValueToBePlacedInOut3.length()-1));
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
	}
	
	public static void callTutorial(String query , String configpath)
	{
    	hs.add("<>");
    	hs.add(">=");
    	hs.add("!>");
    	hs.add(">");
    	hs.add("<=");
    	hs.add("!<");
    	hs.add("<");
    	hs.add("!=");
    	hs.add("=");
    	hs.add("like");
    	hs.add("LIKE");
    	hs.add("Like");
		
    	int valueToDbSystem ;
    	
    	//System.out.println("1");
    	
    	int j=0;
    	int positionsIncolumn=0;
		int numberOfColumnsToBePrinted=0;
		
    	int recordSatisfyingConditionLength=0;
	//	Iterator iterator = hs.iterator();
		tutorial tut = new tutorial();
		System.out.println("in call tutorial:"+configpath);
    	tut.selectMethod(query,configpath);
    	valueToDbSystem = tut.valueToDbSystem;
    	
    	
    	
    	if(valueToDbSystem == 0)
    	{
    		//System.out.println("Query Invalid");
    		return;
    	}
    	strWhereClause = tut.whereClauseValueToDbSystem;
    	columnClause = tut.columnValueToDbSystem;
    	
    	
    	
    	System.out.println("columnclause="+columnClause+"\twhereclause"+strWhereClause);
    	
    	String columnNamesFromSelectionClause[]= columnClause.split(",");
    	//System.out.println("selctionclause="+columnNamesFromSelectionClause.length);
    	tableName = tut.tbName;
    	if( tut.orderByClauseValueToDbSystem.compareToIgnoreCase("NA") != 0 )
    	{	
    		System.out.println("Should not orderby");
    		orderByColumns = tut.orderByClauseValueToDbSystem.split(",") ;
    		orderByColumnsPosition = new int[orderByColumns.length];
    		flagForExistenceOfOrderByClause = true;
    	}
    	
  //  	System.out.println("he"+orderByColumns[0]+"\t" );
    	
    	String operator="";
    	String expressionValue="";
    	
    	
    	
    	dbsystem.readConfig(configpath);
    	//Iterator itr = dbsystem.tableAttributes.entrySet().iterator();

      
    	dbsystem.populateDBInfo();
    //	System.out.println("first");
    	dbsystem.printMap();
    //	System.out.println("second");
    //	System.exit(0);
    //	dbsystem.readConfig(configpath);
    	String PathForData = dbsystem.rf.pathForData;
   // 	System.out.println("tablename="+tableName);
    	lastRecordId = dbsystem.lastRecordIdToBeReturned(tableName+".csv");
    //	System.out.println("last record="+lastRecordId);
    	arr = new int[(int) (lastRecordId+1)];
    	
    	
    //	System.out.println("strwhereclause="+strWhereClause);
    	
    	
    	
    	int recordSatisfyingCondition[] = new int[(int)(lastRecordId+1) ];
    	int temp[] = new int[(int)(lastRecordId+1) ];
    	int temp1[] = new int[(int)(lastRecordId+1) ];
    	int columnNamesToBePrinted[] = new int[100];
    	int noOfColumn=0;
    	try 
    	{
			RandomAccessFile file = new RandomAccessFile(PathForData+tableName+".data", "rw");
			
			String line=file.readLine();
			String Attributes [] = line.split(",");
			
			columnNames = new String[Attributes.length+1];
			
			columnDatatype=new String[Attributes.length+1];
			
			columnNamesToBePrinted = new int[Attributes.length+1];
			
			noOfColumn = Attributes.length;
			
			
			
			for(int i=0; i<Attributes.length;i++)
			{
				String tok[] = Attributes[i].split(":");
				columnNames[i] = tok[0];
				columnDatatype[i] = tok[1];
				
			}
		//	System.out.println("length="+columnNamesFromSelectionClause[0]);
			if(columnNamesFromSelectionClause[0].compareToIgnoreCase("")!=0)
			{
				
				for(int i=0 ;i < columnNamesFromSelectionClause.length;i++)
				{
					for(int l=0 ; l <  columnNames.length ; l++)
					{
						if(columnNamesFromSelectionClause[i].compareToIgnoreCase(columnNames[l])==0 )
						{
							columnNamesToBePrinted[positionsIncolumn] = l;
							positionsIncolumn++;
							break;
						}
					}
				}
			}
			numberOfColumnsToBePrinted = positionsIncolumn;
			//System.out.println("no--"+numberOfColumnsToBePrinted);
			
			int pos = 0;
			if( flagForExistenceOfOrderByClause == true )
			{
				for( int i = 0 ; i < orderByColumns.length ; i++ )
				{
					for( int l = 0 ; l < columnNames.length ; l++ )
					{
						if( orderByColumns[i].compareToIgnoreCase(columnNames[l]) == 0 )
						{
							orderByColumnsPosition[pos] = l;
							pos++;
							break;
						}
					}
				}
				
				numOfOrderByClauseElements = pos;
				
			}
		//	System.out.println("num="+numOfOrderByClauseElements+"\t"+orderByColumnsPosition[0]);
			file.close();
		} 
    	catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    	String op="";
   // 	System.out.println(strWhereClause);
    	for(int i=0;i<strWhereClause.length();i++)
    	{
    		if( (i+4 < strWhereClause.length() && strWhereClause.charAt(i) == ' ' && (strWhereClause.charAt(i+1) == 'a' || strWhereClause.charAt(i+1) == 'A')  && (strWhereClause.charAt(i+2)=='n' || strWhereClause.charAt(i+2) == 'N') && (strWhereClause.charAt(i+3)=='d' || strWhereClause.charAt(i+3) == 'D') && strWhereClause.charAt(i+4) == ' ') || (i+3 <strWhereClause.length() &&  strWhereClause.charAt(i) == ' ' && (strWhereClause.charAt(i+1) == 'o' || strWhereClause.charAt(i+1) == 'O') && (strWhereClause.charAt(i+2) == 'r' || strWhereClause.charAt(i+2) == 'R') && strWhereClause.charAt(i+3) == ' ')) 
    		{
    			
    			if( flag == false)
    			{
    				Iterator iterator = hs.iterator();
    				while (iterator.hasNext())
    			    {
    			         //System.out.println("Value: "+iterator.next() + " ");
    			    	operator=(String)iterator.next();
    			    	if(expression.toLowerCase().contains(operator))
    			    	{
    			    	
    			    		break;
    			    	}  
    			      }
    				  String token[] = expression.split(operator);
    				  expressionValue = token[1];
    				  int position = 0;
    				  for(int k=0 ; k< noOfColumn ;k++)
    				  {
    					  if(token[0].compareToIgnoreCase(columnNames[k])==0)
    					  {
    						  position=k;
    						  break;
    					  }
    				  }
    				  String datatype= columnDatatype[position];
    				  j=0;
    					
    					lengthForReturnRecord=0;
    					numberForReturnRecord=0;
    				  for(int k = 0 ; k <= lastRecordId ;k++ )
    				  {
    			    		strRecord = returnRecord(k);
    			    		String splitRecord[] = strRecord.split(",");
    			    		String recordValue=splitRecord[position];
    			    		if(recordValue.charAt(0)=='"' && recordValue.charAt(recordValue.length()-1)=='"')
    			    		{
    			    			recordValue = recordValue.substring(1,recordValue.length()-1);
    			    		}
    			    		
    			    		boolean b=checkRecord( recordValue , datatype , expressionValue , operator );
    			    		if(b==true)
    			    		{
    			    			recordSatisfyingCondition[j] = k;
    			    			j++;
    			    		}
    			//    		System.out.println("record from"+strRecord);
    			       }
    				  recordSatisfyingConditionLength=j;
    				  flag=true;
    				  expression = "";
    			}
    			else
    			{
    				Iterator iterator = hs.iterator();
    				while (iterator.hasNext())
  			      	{
  			         //System.out.println("Value: "+iterator.next() + " ");
    					operator=(String)iterator.next();
    					if(expression.toLowerCase().contains(operator))
    					{
  			    	
    						break;
    					}  
  			      	}
	    			  String token[] = expression.split(operator);
	  				  expressionValue = token[1];
	  				  int position = 0;
	  				  for(int k=0 ; k< noOfColumn ;k++)
	  				  {
	  					  if(token[0].compareToIgnoreCase(columnNames[k])==0)
	  					  {
	  						  position=k;
	  						  break;
	  					  }
	  				  }
	  				  String datatype= columnDatatype[position];
	  		//		  if(op.compareToIgnoreCase("and")==0)
	  		//			  j=0;
	  		//		  else if(op.compareToIgnoreCase("or")==0)
	  		//			  j=recordSatisfyingConditionLength;
	  				  j=0;

  					lengthForReturnRecord=0;
  					numberForReturnRecord=0;
	  				  for(int k = 0 ; k <= lastRecordId ;k++ )
	  				  {
	  					  	if(op.compareToIgnoreCase("and")==0)
	  					  	{	
	  					  		int l;
	  					  		for(l=0;l<recordSatisfyingConditionLength;l++)
	  					  		{
	  					  			if(k == recordSatisfyingCondition[l])
	  					  			{
	  					  				break;
	  					  			}
	  					  		}
	  					  		if(l == recordSatisfyingConditionLength)
	  					  		{
	  					  			continue;
	  					  		}
	  					  	}
	  					  	else if(op.compareToIgnoreCase("or")==0)
	  					  	{
		  					  	int l;
	  					  		for(l=0;l<recordSatisfyingConditionLength;l++)
	  					  		{
	  					  			if(k == recordSatisfyingCondition[l])
	  					  			{
	  					  				break;
	  					  			}
	  					  		}
	  					  		if(l != recordSatisfyingConditionLength)
	  					  		{
	  					  			continue;
	  					  		}	
	  					  	}
	  			    		strRecord = returnRecord(k);
	  			    		String splitRecord[] = strRecord.split(",");
	  			    		String recordValue=splitRecord[position];
	  			    		if(recordValue.charAt(0)=='"' && recordValue.charAt(recordValue.length()-1)=='"')
	  			    		{
	  			    			recordValue = recordValue.substring(1,recordValue.length()-1);
	  			    		}	  			    		
	  			    		boolean b=checkRecord( recordValue , datatype , expressionValue , operator );
	  			    		
	  			    		if(b==true)
    			    		{
    			    			temp[j] = k;
    			    			j++;
    			    		}
	  			    		
    			       }
	  				  int tempsize=j;
	  				int size=0;
	  				if(op.compareToIgnoreCase("and")==0)
	  				{
	  					
	  					  int l = 0, m = 0;
	  					  while(l < recordSatisfyingConditionLength && m < tempsize)
	  					  {
	  						  if(recordSatisfyingCondition[l] < temp[m])
	  							  l++;
	  						  else if(temp[m] < recordSatisfyingCondition[l])
	  							  m++;
	  						  else /* if arr1[i] == arr2[j] */
	  						  {
	  							  temp1[size]=temp[m];
	  							  m++;
	  							  l++;
	  							  size++;
	  							 
	  						  }
	  					  }
	  					  for(l=0 ;l<size;l++)
	  					  {
	  						  recordSatisfyingCondition[l]=temp1[l];
	  					  }
	  					  recordSatisfyingConditionLength=size;
	  				}
	  				else if(op.compareToIgnoreCase("or")==0)
	  				{
	  					
	  					int l = 0, m = 0;
	  					size=0;
		  				while(l < recordSatisfyingConditionLength && m < tempsize)
		  				{
		  				    	if(recordSatisfyingCondition[l] < temp[m])
		  				    	{  
		  				    		temp1[size]=recordSatisfyingCondition[l];
		  				    		l++;
		  				    		size++;
		  				    	}
			  				    else if(recordSatisfyingCondition[l] > temp[m])
			  				    {
			  				    	temp1[size]=temp[m];
			  				    	m++;
			  				    	size++;
			  				    }
			  				    else
			  				    {
			  				    	temp1[size]=recordSatisfyingCondition[l];
			  				    	l++;
			  				    	m++;
			  				    	size++;
			  				    }
			  				  }
		  				 
		  				  /* Print remaining elements of the larger array */
			  				  while(l < recordSatisfyingConditionLength)
			  				  {
			  					  temp1[size]=recordSatisfyingCondition[l];
			  					  l++;
			  					  size++;
			  				  }
			  				  while(m < tempsize)
			  				  {
			  					  temp1[size]=temp[m];
			  					  m++;
			  					  size++;
			  				  }
			  				  for(l=0 ;l<size;l++)
		  					  {
		  						  recordSatisfyingCondition[l]=temp1[l];
		  					  }
		  					  recordSatisfyingConditionLength=size;
			  				}
		  					 
	  				  
    				  	expression = "";		
    			}
    			if(strWhereClause.charAt(i+1) == 'a' || strWhereClause.charAt(i+1) == 'A' )
    			{	
    				i+=4;
    				op="and";
    			}
    			else if(strWhereClause.charAt(i+1) == 'o' || strWhereClause.charAt(i+1) == 'O' )
    			{	
    				i+=3;
    				op="or";
    			}
    		}
    		else
    		{
    			if( strWhereClause.charAt(i) != ' ' )
    			{
    				expression = expression + strWhereClause.charAt(i);
    			}
    		}
    	}
    	if(expression.compareToIgnoreCase("")!=0)
    	{
    		if(op.compareToIgnoreCase("")==0)
    		{
    			Iterator iterator = hs.iterator();
				  while (iterator.hasNext())
			      {
			         //System.out.println("Value: "+iterator.next() + " ");
			    	operator=(String)iterator.next();
			    	if(expression.contains(operator))
			    	{
			    	
			    		break;
			    	}  
			      }
	//			  System.out.println("oper"+operator);
		//		  System.out.println(expression);
				  String token[] = expression.split(operator);
				  expressionValue = token[1];
				  int position = 0;
				  for(int k=0 ; k< noOfColumn ;k++)
				  {
					  if(token[0].compareToIgnoreCase(columnNames[k])==0)
					  {
						  position=k;
						  break;
					  }
				  }
				  String datatype= columnDatatype[position];
				  j=0;

					lengthForReturnRecord=0;
					numberForReturnRecord=0;
				  for(int k = 0 ; k <= lastRecordId ;k++ )
				  {
			    		strRecord = returnRecord(k);
			    		String splitRecord[] = strRecord.split(",");
			    		String recordValue=splitRecord[position];
			    		if(recordValue.charAt(0)=='"' && recordValue.charAt(recordValue.length()-1)=='"')
			    		{
			    			recordValue = recordValue.substring(1,recordValue.length()-1);
			    		}
			    		
			    		boolean b=checkRecord( recordValue , datatype , expressionValue , operator );
			    		if(b==true)
			    		{
			    			recordSatisfyingCondition[j] = k;
			    			j++;
			    		}
			    	//	System.out.println(strRecord);
			       }
				  recordSatisfyingConditionLength=j;
			
    		}
    		else 
    		{
    			Iterator iterator = hs.iterator();
				while (iterator.hasNext())
			    {
			         //System.out.println("Value: "+iterator.next() + " ");
					operator=(String)iterator.next();
					if(expression.toLowerCase().contains(operator))
					{
			    	
						break;
					}  
			     }
    			 String token[] = expression.split(operator);
  				 expressionValue = token[1];
  				 int position = 0;
  				 for(int k=0 ; k< noOfColumn ;k++)
  				 {
  					  if(token[0].compareToIgnoreCase(columnNames[k])==0)
  					  {
  						  position=k;
  						  break;
  					  }
  				 }
  				  String datatype= columnDatatype[position];
  		//		  if(op.compareToIgnoreCase("and")==0)
  		//			  j=0;
  		//		  else if(op.compareToIgnoreCase("or")==0)
  		//			  j=recordSatisfyingConditionLength;
  				  j=0;

					lengthForReturnRecord=0;
					numberForReturnRecord=0;
  				  for(int k = 0 ; k <= lastRecordId ;k++ )
  				  {
  					  	if(op.compareToIgnoreCase("and")==0)
  					  	{	
  					  		int l;
  					  		for(l=0;l<recordSatisfyingConditionLength;l++)
  					  		{
  					  			if(k == recordSatisfyingCondition[l])
  					  			{
  					  				break;
  					  			}
  					  		}
  					  		if(l == recordSatisfyingConditionLength)
  					  		{
  					  			continue;
  					  		}
  					  	}
  					  	else if(op.compareToIgnoreCase("or")==0)
  					  	{
	  					  	int l;
  					  		for(l=0;l<recordSatisfyingConditionLength;l++)
  					  		{
  					  			if(k == recordSatisfyingCondition[l])
  					  			{
  					  				break;
  					  			}
  					  		}
  					  		if(l != recordSatisfyingConditionLength)
  					  		{
  					  			continue;
  					  		}	
  					  	}
  			    		strRecord = returnRecord(k);
  			    		String splitRecord[] = strRecord.split(",");
  			    		String recordValue=splitRecord[position];
  			    		if(recordValue.charAt(0)=='"' && recordValue.charAt(recordValue.length()-1)=='"')
  			    		{
  			    			recordValue = recordValue.substring(1,recordValue.length()-1);
  			    		}	  			    		
  			    		boolean b=checkRecord( recordValue , datatype , expressionValue , operator );
  			    		
  			    		if(b==true)
			    		{
			    			temp[j] = k;
			    			j++;
			    		}
  			    		
			       }
  				  int tempsize=j;
  				  int size=0;
  				  if(op.compareToIgnoreCase("and")==0)
  				  {
  					
	  					  int l = 0, m = 0;
	  					  while(l < recordSatisfyingConditionLength && m < tempsize)
	  					  {
	  						  if(recordSatisfyingCondition[l] < temp[m])
	  							  l++;
	  						  else if(temp[m] < recordSatisfyingCondition[l])
	  							  m++;
	  						  else /* if arr1[i] == arr2[j] */
	  						  {
	  							  temp1[size]=temp[m];
	  							  m++;
	  							  l++;
	  							  size++;
	  							 
	  						  }
	  					  }
						  for(l=0 ;l<size;l++)
						  {
							  recordSatisfyingCondition[l]=temp1[l];
						  }
						  recordSatisfyingConditionLength=size;
  				}
  				else if(op.compareToIgnoreCase("or")==0)
  				{
  					
	  					int l = 0, m = 0;
	  					size=0;
		  				while(l < recordSatisfyingConditionLength && m < tempsize)
		  				{
		  				    	if(recordSatisfyingCondition[l] < temp[m])
		  				    	{  
		  				    		temp1[size]=recordSatisfyingCondition[l];
		  				    		l++;
		  				    		size++;
		  				    	}
			  				    else if(recordSatisfyingCondition[l] > temp[m])
			  				    {
			  				    	temp1[size]=temp[m];
			  				    	m++;
			  				    	size++;
			  				    }
			  				    else
			  				    {
			  				    	temp1[size]=recordSatisfyingCondition[l];
			  				    	l++;
			  				    	m++;
			  				    	size++;
			  				    }
			  				  }
		  				 
		  				  /* Print remaining elements of the larger array */
			  				  while(l < recordSatisfyingConditionLength)
			  				  {
			  					  temp1[size]=recordSatisfyingCondition[l];
			  					  l++;
			  					  size++;
			  				  }
			  				  while(m < tempsize)
			  				  {
			  					  temp1[size]=temp[m];
			  					  m++;
			  					  size++;
			  				  }
			  				  for(l=0 ;l<size;l++)
		  					  {
		  						  recordSatisfyingCondition[l]=temp1[l];
		  					  }
		  					  recordSatisfyingConditionLength=size;
		  		}
	  					 
  				  
				  
			}
		}
    	//positionsIncolumn=0;
    	//columnNamesToBePrinted
    	String result = "";
    	String result1 = "";
    	int sizeOfInsertion =  dbsystem.rf.pageSize * dbsystem.rf.numOfPages;
    //	System.out.println("hello");
   // 	System.out.println("size="+sizeOfInsertion);
    		try 
    		{
				
    			
    			
//    			dumpFile = new RandomAccessFile("/home/salman/Videos/f1.txt", "rw");
	//			dumpFileInDuplicate = new RandomAccessFile("/home/salman/Videos/f2.txt", "rw");
				
    			
    			
				fstream1 = new FileWriter(PathForData+"f1.txt",false);
				
				out1 = new BufferedWriter(fstream1);	
				
				fstream2 = new FileWriter(PathForData+"f2.txt",false);
				
				out2 = new BufferedWriter(fstream2);
				
				fstream3 = new FileWriter(PathForData+"f3.txt",false);
				
				out3 = new BufferedWriter(fstream3);	
				
				boolean checkFirsttime=false;
				System.out.println("record="+recordSatisfyingConditionLength);

				lengthForReturnRecord=0;
				numberForReturnRecord=0;
				for(int i=0 ; i<recordSatisfyingConditionLength ;i++ )
				{
					//	if(i==columnNamesToBePrinted[positionsIncolumn])
				
					strRecord = returnRecord(recordSatisfyingCondition[i]);
					if(columnClause.compareToIgnoreCase("")==0)
					{	
						//System.out.println(strRecord);
						if(flagForExistenceOfOrderByClause==true)
						{
						//	System.out.println("ppp");
							
							if( (result1 + strRecord + "\n").length() > sizeOfInsertion )
							{
									
								//	String strToBeSorted[] = result1.split("\n");
									
									
									
							//		System.out.println("result1\n"+result1);
									//out2.write(result.substring(0, result.length()-1));
									if(checkFirsttime ==  false)
									{
										//merging
										
										String sortedData = sort(result1);
								//		System.out.println("sortde\n"+sortedData);
										out1.write(sortedData);
										
									}
									else
									{
									//	System.out.println("result1\n"+result1);
										String sortedData = sort(result1);
										out2.close();
										fstream2 = new FileWriter(PathForData+"f2.txt",false);
										out2 = new BufferedWriter(fstream2);
							//			System.out.println("sorteddata\n"+sortedData);
										out2.write(sortedData);
										
										//merge out1 and out2 into out3 and then out1 = out3
										merge(PathForData);
										
									}
									checkFirsttime = true;
									
									result1 = "";
							}
						}		
						result += strRecord + "\n";
						result1 += strRecord+"\n";
						
						

					}
					else
					{
						String tok[] = strRecord.split(",");
					//	result += strRecord + "\n";
						result1 += strRecord+"\n";
						
						//--System.out.println("no="+numberOfColumnsToBePrinted);
						for(int l=0 ; l < numberOfColumnsToBePrinted ; l++ )
						{	
								//System.out.print(tok[columnNamesToBePrinted[l]]);
							result += tok[columnNamesToBePrinted[l]];
						//	result1 += tok[columnNamesToBePrinted[l]];
							
							if(l!=numberOfColumnsToBePrinted-1)
							{	
									//System.out.print(",");
								result += ",";
							//	result1 +=",";
								
							}
						}
						
						result += "\n";
						//result1 += "\n";
						if(flagForExistenceOfOrderByClause==true)
						{
							if( (result1).length() > sizeOfInsertion )
							{
									//dump into file
									//merge sort
							//	String strToBeSorted[] = result1.split("\n");
								
								//out2.write(result.substring(0, result.length()-1));
								if(checkFirsttime ==  false)
								{
									//merging
									String sortedData = sort(result1);
									out1.write(sortedData);
									
								}
								else
								{
									String sortedData = sort(result1);
									out2.close();
									fstream2 = new FileWriter(PathForData+"f2.txt",false);
									out2 = new BufferedWriter(fstream2);
						//			System.out.println("sorteddata\n"+sortedData);
									out2.write(sortedData);
									
									//merge out1 and out2 into out3 and then out1 = out3
									merge(PathForData);
								
								}
								checkFirsttime = true;
							
								
								result1 = "";
							}
						}
							
							//System.out.println();
					}
				}
				//System.out.println("r="+result1);
				if(result1.compareToIgnoreCase("")!=0 && flagForExistenceOfOrderByClause==true)
				{
					String sortedData = sort(result1);
					out2.close();
					fstream2 = new FileWriter(PathForData+"f2.txt",false);
					out2 = new BufferedWriter(fstream2);
					//System.out.println("sorteddata\n"+sortedData);
					out2.write(sortedData);
					
					//merge out1 and out2 into out3 and then out1 = out3
					merge(PathForData);
					result1="";
				}
				if(strWhereClause.compareToIgnoreCase("")==0)
				{
					//	System.out.println("ngjhn1");
						result1="";
						result="";

    					lengthForReturnRecord=0;
    					numberForReturnRecord=0;
						for(int i=0 ; i<=lastRecordId ;i++ )
						{
							
							strRecord = returnRecord(i);
						//	result += strRecord;
						//	result1 += strRecord;
						//	System.out.println("col="+columnClause);
							if(columnClause.compareToIgnoreCase("")==0)
							{	
								if(flagForExistenceOfOrderByClause==true)
								{
									if( (result1 + strRecord + "\n").length() > sizeOfInsertion )
									{
										//dump in file
										//merge sort
									//	String strToBeSorted[] = result1.split("\n");
										
										
										
										
										//out2.write(result.substring(0, result.length()-1));
										if(checkFirsttime ==  false)
										{
											//merging
											String sortedData = sort(result1);
											out1.write(sortedData);
											
										}
										else
										{
											String sortedData = sort(result1);
											out2.close();
											fstream2 = new FileWriter(PathForData+"f2.txt",false);
											out2 = new BufferedWriter(fstream2);
						//					System.out.println("sorteddata\n"+sortedData);
											out2.write(sortedData);
											
											//merge out1 and out2 into out3 and then out1 = out3
											merge(PathForData);
										}
										checkFirsttime = true;
									
										result1 = "";
									
									}
								//System.out.println(strRecord);
								//	result += strRecord + "\n";
								//	result1 += strRecord + "\n";
								}
								result += strRecord + "\n";
								result1 += strRecord + "\n";
							}		
						else
						{
						//		result += strRecord;
								result1 += strRecord;
								String tok[] = strRecord.split(",");
								
						//		System.out.println("no="+numberOfColumnsToBePrinted);
								for(int l=0 ; l < numberOfColumnsToBePrinted ; l++ )
								{	
									//System.out.print(tok[columnNamesToBePrinted[l]]);
									result += tok[columnNamesToBePrinted[l]];
								//	result1 += tok[columnNamesToBePrinted[l]];
									if(l!=numberOfColumnsToBePrinted-1)
									{	
										//System.out.print(",");
										result += ",";
								//		result1 += ",";
									}
								}
							
								result += "\n";
								result1 += "\n";
								//System.out.println();
								if(flagForExistenceOfOrderByClause==true)
								{
									if( (result1).length() > sizeOfInsertion )
									{
										//dump into file
										//merge sort
										if(checkFirsttime ==  false)
										{
											//merging
											String sortedData = sort(result1);
											out1.write(sortedData);
											
										}
										else
										{
											String sortedData = sort(result1);
											out2.close();
											fstream2 = new FileWriter(PathForData+"f2.txt",false);
											out2 = new BufferedWriter(fstream2);
					//						System.out.println("sorteddata\n"+sortedData);
											out2.write(sortedData);
											
											//merge out1 and out2 into out3 and then out1 = out3
											merge(PathForData);
										
											
										}
										checkFirsttime = true;
									
										result1 = "";
									}
								}
							}
						
						}
					}
				if(result1.compareToIgnoreCase("")!=0 && flagForExistenceOfOrderByClause==true)
				{
					//System.out.println("res="+result1);
					String sortedData = sort(result1);
					out2.close();
					fstream2 = new FileWriter(PathForData+"f2.txt",false);
					out2 = new BufferedWriter(fstream2);
					//System.out.println("sorteddata\n"+sortedData);
					out2.write(sortedData);
					
					//merge out1 and out2 into out3 and then out1 = out3
					merge(PathForData);
					result1="";
				}
				out1.close();
				out2.close();
				out3.close();
		  //	System.out.println("---------");
			if( flagForExistenceOfOrderByClause == false )
				System.out.print(result);
			else
			{
				RandomAccessFile rf = new RandomAccessFile(PathForData+"f1.txt", "r");
				
				if(columnClause.compareToIgnoreCase("")==0)
				{
					String line="";
					
					while( (line = rf.readLine()) != null )
					{
						System.out.println(line);
					}
				}
				else
				{
					String line="";
					
					String printValue = "";
					
					while( (line = rf.readLine()) != null )
					{
						String tok[] = line.split(",");
						
						for( int i = 0 ; i < numberOfColumnsToBePrinted ; i++ )
						{
							printValue += (tok[columnNamesToBePrinted[i]]+",");
						}
						
						printValue = printValue.substring(0,printValue.length()-1);
						printValue += "\n";
					}
					
					printValue = printValue.substring(0, printValue.length() - 1);
					
					System.out.println(printValue);
					
					
				}
				
			}
		  	
		  //  	System.out.println(columnClause);
		} 
    	catch (Exception e) 
    	{
				// TODO Auto-generated catch block
				e.printStackTrace();
		} 
	}	
}
