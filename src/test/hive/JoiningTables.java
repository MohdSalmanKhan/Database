package test.hive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;



//import com.salman.main.myComp;


class myComp1 implements Comparator<String> { @Override public int compare(String n1, String n2) { if(n1.compareToIgnoreCase(n2) > 0) { return 1; } else { return -1; } } }

class myComp2 implements Comparator<Integer> { @Override public int compare(Integer n1,Integer n2) { if(n1 > n2) { return 1; } else { return -1; } } }

class myComp3 implements Comparator<Float> { @Override public int compare(Float n1, Float n2) { if(n1 > n2) { return 1; } else { return -1; } } }


public class JoiningTables {

	public static HashSet<String> hs = new HashSet<String>();
	public static String operator="";
	public static String attribute1="",attribute2="";
	public static String configFilePath="";
	public static TreeMap<String,String> treeMapForVarchar = new TreeMap<String, String>(new myComp1());
	public static TreeMap<Integer,String> treeMapForInt = new TreeMap<Integer, String>(new myComp2());
	public static TreeMap<Float,String> treeMapForFloat = new TreeMap<Float, String>(new myComp3());
	public static int recordLimit = 40000;
	public static String pathForData = "";
	public static String dataType1="",dataType2="",dataType="";
	public static int fileCount1=0,fileCount2=0,fcount=0;
	public static int numberOfRecords1=0,numberOfRecords2=0,numberOfRecords=0;
	
	public static int getAttributeNumber(String tableName,String Attribute) 
	{
	
		
		//System.out.println(tableName+"\t"+Attribute);
		int number=0;
		try {
			RandomAccessFile raf = new RandomAccessFile(configFilePath, "r");
			String line;
			boolean flag=false;
			number = 0;
			
			while((line=raf.readLine())!=null)
			{
				if(flag == true)
				{
					String token[] = line.split(",");
					token[0]=token[0].trim();
					//System.out.println("ID="+token[0]);
					if(token[0].compareToIgnoreCase(Attribute.trim())==0)
					{
						if(token[1].toLowerCase().contains("varchar"))
						{
							dataType="varchar";
						}
						else
							dataType = token[1];
						break;
					}
					number++;
				}
				
				if(line.compareToIgnoreCase(tableName)==0)
				{
					flag= true;
			//		System.out.println("hello");
					number=0;
				}
				if(line.compareToIgnoreCase("end")==0)
					flag = false;
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return number;
	}
	public static int getAtt(String tableName,String Attribute) 
	{
			
		int number=0;
		boolean returnFlag=false;
		try {
			RandomAccessFile raf = new RandomAccessFile(configFilePath, "r");
			String line;
			boolean flag=false;
			number = 0;
			
			while((line=raf.readLine())!=null)
			{
				if(flag == true)
				{
					String token[] = line.split(",");
					token[0]=token[0].trim();
					if(token[0].compareToIgnoreCase(Attribute.trim())==0)
					{
				//			System.out.println("3");
							returnFlag = true;
							break;
					}
					number++;
				}
				
				if(line.compareToIgnoreCase(tableName)==0)
				{
			//		System.out.println("2");
					flag= true;
					number=0;
				}
				if(line.compareToIgnoreCase("end")==0)
				{
					flag= false;
				}
				
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(returnFlag==false)
			return -1;
		return number;
	}

	
	public static void sortTable(String tableName,int attrNumber,String datatype)
	{
		int lineCount = 0;
		int filenumber=0;
		try
		{
	//		System.out.println(pathForData);
			RandomAccessFile raf = new RandomAccessFile(pathForData+tableName+".csv", "r");
			System.out.println("-----"+pathForData+tableName+".csv");
			//System.out.println("h");
			String line=raf.readLine();
			
			System.out.println("attr="+attrNumber);
			
			while( line != null )
			{
				
		//		System.out.println("line="+line);
				String tok[] = line.split(",");
				lineCount++;
			//	System.out.println("toke="+attrNumber);
				
				if(datatype.trim().toLowerCase().contains("varchar"))
				{
					treeMapForVarchar.put(tok[attrNumber], line);
				}
				else if( datatype.trim().toLowerCase().contains("integer") )
				{
					if(tok[attrNumber].charAt(0)=='"')
					{
						tok[attrNumber] = tok[attrNumber].substring(1, tok[attrNumber].length()-1);
						
					}
					treeMapForInt.put(Integer.parseInt(tok[attrNumber]), line);
				}
				else
				{
					if(tok[attrNumber].charAt(0)=='"')
					{
						tok[attrNumber] = tok[attrNumber].substring(1, tok[attrNumber].length()-1);
						
					}
		
					treeMapForFloat.put(Float.parseFloat(tok[attrNumber]), line);
				}
				
				
	
				if(lineCount == recordLimit)
				{
					FileWriter fw = new FileWriter(new File(pathForData+tableName+filenumber+".txt"));
					filenumber++;
					
					if(datatype.trim().toLowerCase().contains("varchar"))
					{
						Iterator<Map.Entry<String, String>> entries = treeMapForVarchar.entrySet().iterator();
						while (entries.hasNext()) {
						    Map.Entry<String, String> entry = entries.next();
						 
						    fw.write(entry.getValue()+"\n");
						}
						treeMapForVarchar.clear();
					}
					else if(datatype.trim().toLowerCase().contains("integer"))
					{
						Iterator<Map.Entry<Integer, String>> entries = treeMapForInt.entrySet().iterator();
						while (entries.hasNext()) {
						    Map.Entry<Integer, String> entry = entries.next();
						 
						    fw.write(entry.getValue()+"\n");
						}
						treeMapForInt.clear();
					}
					else
					{
						Iterator<Map.Entry<Float, String>> entries = treeMapForFloat.entrySet().iterator();
						while (entries.hasNext()) {
						    Map.Entry<Float, String> entry = entries.next();
						 
						    fw.write(entry.getValue()+"\n");
						}
						treeMapForFloat.clear();
					}
					lineCount=0;
					fw.close();
					
					
				}
				line = raf.readLine();
			}
			FileWriter fw = new FileWriter(new File(pathForData+tableName+filenumber+".txt"));
			filenumber++;
			
			if(datatype.trim().toLowerCase().contains("varchar"))
			{
				Iterator<Map.Entry<String, String>> entries = treeMapForVarchar.entrySet().iterator();
				while (entries.hasNext()) {
				    Map.Entry<String, String> entry = entries.next();
		
				    fw.write(entry.getValue()+"\n");
				}
				fw.close();
				treeMapForVarchar.clear();
			}
			else if(datatype.trim().toLowerCase().contains("integer"))
			{
				Iterator<Map.Entry<Integer, String>> entries = treeMapForInt.entrySet().iterator();
				while (entries.hasNext()) {
				    Map.Entry<Integer, String> entry = entries.next();
				 
				    fw.write(entry.getValue()+"\n");
				}
				fw.close();
				treeMapForInt.clear();
			}
			else
			{
				Iterator<Map.Entry<Float, String>> entries = treeMapForFloat.entrySet().iterator();
				while (entries.hasNext()) {
				    Map.Entry<Float, String> entry = entries.next();
				 
				    fw.write(entry.getValue()+"\n");
				}
				fw.close();
				treeMapForFloat.clear();
			}

			fcount = filenumber;
		}
		catch(Exception e)
		{
			System.out.println(e.getStackTrace());
		}
	}
	
	
	public static void merge(String table,int AttributeNumber,String datatype,int fileCount,String path)
	{
		int i;
		try
		{
			numberOfRecords = 0;
			String change="",min="";
			String required[]=new String[fileCount];
			String other[]=new String[fileCount];
			BufferedReader[] brs = new BufferedReader[fileCount];
			for(i=0;i<fileCount;i++)
			{			
				brs[i] = new BufferedReader(new FileReader(path+table+i+".txt"));		
			}
			FileWriter fstream = new FileWriter(path+table+"_final.txt");
			BufferedWriter out = new BufferedWriter(fstream);
			
			
		
			for(i=0;i<fileCount;i++)
			{
				if((change=brs[i].readLine())!=null)
				{	
					String tok[] = change.split(",");
					required[i] = tok[AttributeNumber];
					other[i] = change;
				}
			}
			while(true)
			{
				min=null;
				for(i=0;i<fileCount;i++)
				{
					if(required[i]!=null)
					{
						min=required[i];
						break;
					}
				}
				
				if(min==null)
				{		
					break;
				}
				for(i=0;i<fileCount;i++)
				{
					if(required[i]==null)
						continue;
					if(datatype.toLowerCase().contains("varchar"))
					{
						if(required[i].compareTo(min) < 0)
						{
							min=required[i];
						}
					}
					else if(datatype.toLowerCase().contains("int"))
					{
						String str=required[i];
						if(required[i].charAt(0)=='"')
							str = required[i].substring(1,required[i].length()-1);
						int number1 = Integer.parseInt(str);

						if(min.charAt(0)=='"')
							min = min.substring(1,min.length()-1);

						int number2 = Integer.parseInt(min);
						if(number1 < number2)
						{
							min = String.valueOf(number1);
						}
					}
					else
					{
						String str=required[i];
						if(required[i].charAt(0)=='"')
							str = required[i].substring(1,required[i].length()-1);
						
						if(min.charAt(0)=='"')
							min = min.substring(1,min.length()-1);

						
						
						float number1 = Float.parseFloat(str);
						float number2 = Float.parseFloat(min);
						if(number1 < number2)
						{
							min = String.valueOf(number1);
						}
					}
				}
						
				for(i=0;i<fileCount;i++)
				{	
					if(required[i]==null)
						continue;
					
					if(datatype.toLowerCase().contains("varchar"))
					{
						if(required[i].compareTo(min) == 0)
						{	
							
							out.write(other[i]+"\n");
							numberOfRecords++;
							if((change=brs[i].readLine())!=null)
							{
	
								String tok[] = change.split(",");
								required[i] = tok[AttributeNumber];
								other[i] = change;
							}
							else
							{
							//	str[i-1]=null;
								required[i]=null;
								other[i]=null;
							}
						}
						
					}
					else if(datatype.toLowerCase().contains("int"))
					{
						String str = required[i].substring(1,required[i].length()-1);
						int number1 = Integer.parseInt(str);
						int number2 = Integer.parseInt(min);
						
						
						if( number1 == number2)
						{	
							numberOfRecords++;
							out.write(other[i]+"\n");
							if((change=brs[i].readLine())!=null)
							{
	
								String tok[] = change.split(",");
								required[i] = tok[AttributeNumber];
								other[i] = change;
							}
							else
							{
							//	str[i-1]=null;
								required[i]=null;
								other[i]=null;
							}
						}
	
					}
					else
					{
						String str = required[i].substring(1,required[i].length()-1);
						float number1 = Float.parseFloat(str);
						float number2 = Float.parseFloat(min);
						
						
						if( number1 == number2 )
						{	
							numberOfRecords++;
							out.write(other[i]+"\n");
							if((change=brs[i].readLine())!=null)
							{
	
								String tok[] = change.split(",");
								required[i] = tok[AttributeNumber];
								other[i] = change;
							}
							else
							{
							//	str[i-1]=null;
								required[i]=null;
								other[i]=null;
							}
						}
	
					}
				}
			}
			out.close();
		}
		catch(Exception e)
		{
			System.out.println("Exception:"+e );
		}
	}
	
	
	public static void concat(String str1,String str2,int number1,int number2,String columnValue,String table1,String table2)
	{
		
		
	//	System.out.println("str1="+str1+"\tstr2="+str2);
		String token[] = str2.split(",");
		String answer="";
		int index1,index2;
	//	System.out.println("tabl:"+table1);
		if(columnValue.contains("*"))
		{
			String strOfJoin = str1;
			for(int i=0;i<token.length;i++)
			{
				if(i==number2)
					continue;
				strOfJoin += "," + token[i]; 
			}
			System.out.println(strOfJoin);
		}			
		else
		{
			String tok[] = columnValue.split(",");
			
			for(int i=0;i<tok.length;i++)
			{
				index1 = -1;
				index2 = -1;
			//	System.out.println(tok[i]);
				if(tok[i].contains("."))
				{
					//find index
					String str[] = tok[i].split("\\.");
					if(str[0].compareToIgnoreCase(table1) == 0)
						index1 = getAtt(table1,str[1]);
					else if(str[0].compareToIgnoreCase(table2) == 0)
					{
		//				System.out.println("MATCHED");
						index2 = getAtt(table2,str[1]);
			//			System.out.println("i="+index2);
					}
				}
				else
				{
					// find index
			//		System.out.println("1");
			//		if(tok[i].compareToIgnoreCase(table1) == 0)
						index1 = getAtt(table1,tok[i]);
				//	else if (tok[i].compareToIgnoreCase(table1) == 0)
						index2 = getAtt(table2,tok[i]);
				}
				if(index1 == -1 && index2 ==-1)
				{
					
					System.out.println("Invalid Query1");
				}
				else if(index1 !=-1 && index2!= -1)
				{
					System.out.println("Invalid Query2");
				}
				else if(index1 == -1)
				{
			//		System.out.println("index="+index2);
			//		System.out.println("str="+str2);
					String str[] = str2.split(",");
					//System.out.println(str[index2]);
					answer += str[index2] +",";
				}
				else
				{
		//			System.out.println("1index="+index1);
		//			System.out.println("1str="+str1);
					String str[] = str1.split(",");
				//	System.out.println(str[index1]);
					answer += str[index1] + ",";
				}
			}
		}
		System.out.println(answer.substring(0, answer.length()-1));
	}
	
	public static void join(String table1,String table2,String pathForData,int numberAtt1,int numberAtt2,String datatype,String columnValue)
	{
		
		String smallRelationArray[];
		System.out.println("results\n\n");
		
		System.out.println(numberOfRecords1+"\t"+numberOfRecords2);
		
		if(numberOfRecords1 > numberOfRecords2)
			smallRelationArray = new String[numberOfRecords2];
		else
			smallRelationArray = new String[numberOfRecords1];
		
		try
		{
			RandomAccessFile rf1 = new RandomAccessFile(pathForData+table1+"_final.txt", "r");
			RandomAccessFile rf2 = new RandomAccessFile(pathForData+table2+"_final.txt", "r");
			File f1 = new File(pathForData+table1+"_final.txt");
			File f2 = new File(pathForData+table2+"_final.txt");
			long size1 = f1.length();
			long size2 = f2.length();
			if(size1 < size2)
			{
				String line="";
				int j=0;
				while((line=rf1.readLine())!=null)
				{
					smallRelationArray[j] = line;
					j++;
				}
				int i=0;
				int temp=0;
				while((line=rf2.readLine())!=null)
				{
					String token[] = line.split(",");
					String AttributeValue = token[numberAtt2];
					String record="";
					i=temp;
					boolean flag = false;
					for( ; i<numberOfRecords1;i++)
					{
						if(datatype.toLowerCase().contains("varchar"))
						{	
							String tok[] = smallRelationArray[i].split(",");
							record = tok[numberAtt1];
							if(record.compareToIgnoreCase(AttributeValue) < 0)
							{
								
								continue;
							}
							else if(record.compareToIgnoreCase(AttributeValue) > 0)
							{
								break;
							}
							else
							{
								concat(line,smallRelationArray[i],numberAtt1,numberAtt2,columnValue,table1,table2);
								if(flag == false)
									temp = i;
								flag = true;
							}
						}
						else if(datatype.toLowerCase().contains("int"))
						{
							String tok[] = smallRelationArray[i].split(",");
							record = tok[numberAtt1];
							if(record.charAt(0)=='"')
								record = record.substring(1,record.length()-1);
							
							int number1 = Integer.parseInt(record);
							
							if(AttributeValue.charAt(0)=='"')
								AttributeValue = AttributeValue.substring(1,AttributeValue.length()-1);
							
							int number2 = Integer.parseInt(AttributeValue);
							
							if( number1 < number2 )
							{
								
								continue;
							}
							else if( number1 >  number2 )
							{
								break;
							}
							else
							{
								concat(line,smallRelationArray[i],numberAtt1,numberAtt2,columnValue,table1,table2);
								if(flag == false)
									temp = i;
								flag = true;
							}
						}
						else
						{
							String tok[] = smallRelationArray[i].split(",");
							record = tok[numberAtt1];
							if(record.charAt(0)=='"')
								record = record.substring(1,record.length()-1);
							
							float number1 = Float.parseFloat(record);
							
							if(AttributeValue.charAt(0)=='"')
								AttributeValue = AttributeValue.substring(1,AttributeValue.length()-1);
							
							float number2 = Float.parseFloat(AttributeValue);
							
							if( number1 < number2 )
							{
								
								continue;
							}
							else if( number1 >  number2 )
							{
								break;
							}
							else
							{
								concat(line,smallRelationArray[i],numberAtt1,numberAtt2,columnValue,table1,table2);
								if(flag == false)
									temp = i;
								flag = true;
							}
						}
					}
					if(flag == false)
					{
						if(numberOfRecords1 == i)
							break;
						temp = i;
					}
				}
			}
			else
			{
				String line="";
				int j=0;
				while((line=rf2.readLine())!=null)
				{
					smallRelationArray[j] = line;
					j++;
				}
				
				int i=0;
				int temp=0;
				while((line=rf1.readLine())!=null)
				{
					String token[] = line.split(",");
					String AttributeValue = token[numberAtt1];
					String record="";
					i=temp;
					boolean flag = false;
					for( ; i<numberOfRecords2;i++)
					{
						if(datatype.toLowerCase().contains("varchar"))
						{	
							String tok[] = smallRelationArray[i].split(",");
							record = tok[numberAtt2];
							if(record.compareToIgnoreCase(AttributeValue) < 0)
							{
								
								continue;
							}
							else if(record.compareToIgnoreCase(AttributeValue) > 0)
							{
								break;
							}
							else
							{
								concat(line,smallRelationArray[i],numberAtt1,numberAtt2,columnValue,table1,table2);
								if(flag == false)
									temp = i;
								flag = true;
							}
						}
						else if(datatype.toLowerCase().contains("int"))
						{
							String tok[] = smallRelationArray[i].split(",");
							record = tok[numberAtt2];
							if(record.charAt(0)=='"')
								record = record.substring(1,record.length()-1);
							
							int number1 = Integer.parseInt(record);
							
							if(AttributeValue.charAt(0)=='"')
								AttributeValue = AttributeValue.substring(1,AttributeValue.length()-1);
							
							int number2 = Integer.parseInt(AttributeValue);
							
							if( number1 < number2 )
							{
								
								continue;
							}
							else if( number1 >  number2 )
							{
								break;
							}
							else
							{
								concat(line,smallRelationArray[i],numberAtt1,numberAtt2,columnValue,table1,table2);
								if(flag == false)
									temp = i;
								flag = true;
							}
						}
						else
						{
							String tok[] = smallRelationArray[i].split(",");
							record = tok[numberAtt2];
							if(record.charAt(0)=='"')
								record = record.substring(1,record.length()-1);
							
							float number1 = Float.parseFloat(record);
							
							if(AttributeValue.charAt(0)=='"')
								AttributeValue = AttributeValue.substring(1,AttributeValue.length()-1);
							
							float number2 = Float.parseFloat(AttributeValue);
							
							if( number1 < number2 )
							{
								
								continue;
							}
							else if( number1 >  number2 )
							{
								break;
							}
							else
							{
								concat(line,smallRelationArray[i],numberAtt1,numberAtt2,columnValue,table1,table2);
								if(flag == false)
									temp = i;
								flag = true;
							}
						}			}
					if(flag == false)
					{
						if(numberOfRecords2 == i)
							break;
						temp =i;
					}
				}
				
			}
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
	public static void joiningResult(String table1,String table2,String joiningCondition,String columnValueToDbSystem,String path,String configPath)
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
    	
    	
    	//System.out.println("jc:"+joiningCondition);
    	
    	configFilePath =  configPath;
    	pathForData = path;
    	Iterator<String> iterator = hs.iterator();
    	while (iterator.hasNext())
    	{
    		
    		String str =(String)iterator.next();
    		if(joiningCondition.contains(str))
    		{
    			operator = str;
    			break;
    		}
    		
        }
    	
    	String token[] = joiningCondition.split(operator);
    	
    	if(token[0].contains("."))
    	{
    		if(token[0].toLowerCase().contains(table1.toLowerCase()))
    		{
    			String tok[] = token[0].split("\\.");
    			attribute1 = tok[1];
    		}
    		else
    		{

    			String tok[] = token[0].split("\\.");
    			attribute2 = tok[1];
    		}
    	}
    	else
    	{
    		attribute1 = token[0];
    	}
    	
    	if(token[1].contains("."))
    	{
    		if(token[1].toLowerCase().contains(table2.toLowerCase()))
    		{
	    		String tok[] = token[1].split("\\.");
	    		attribute2 = tok[1];
    		}
    		else
    		{

	    		String tok[] = token[1].split("\\.");
	    		attribute1 = tok[1];
    		}
    	}
    	else
    	{
    		attribute2 = token[1];
    	}
    //	System.out.println("1="+attribute1);
    //	System.out.println("2="+attribute2);
    	int numberAtt1=getAttributeNumber(table1,attribute1);
    	dataType1 = dataType;
    	int numberAtt2=getAttributeNumber(table2,attribute2);
    	dataType2 = dataType;
    	System.out.println("d1="+dataType1+"\td2="+dataType2);
    	
    	if(dataType1.trim().compareToIgnoreCase(dataType2.trim()) != 0)
    	{
    		System.out.println("Invalid Query");
    		return;
    	}
    	
 
    	System.out.println("num="+numberAtt1+"\t"+numberAtt2);
    	sortTable(table1,numberAtt1,dataType1);
    	fileCount1 = fcount;
    	
    	merge(table1,numberAtt1,dataType1,fileCount1,pathForData);
    	numberOfRecords1 = numberOfRecords;
    	sortTable(table2,numberAtt2,dataType2);
    	
    	fileCount2 = fcount;
    	merge(table2,numberAtt2,dataType2,fileCount2,pathForData);
    	numberOfRecords2 = numberOfRecords;
    	
    	
    	
    	
    	join(table1,table2,pathForData,numberAtt1,numberAtt2,dataType1,columnValueToDbSystem);
    	
    	System.out.println(columnValueToDbSystem);
    	
    	
	}
	
	
}
