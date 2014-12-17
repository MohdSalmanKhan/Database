package com.db.main;

import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import test.hive.QueryChecker;

public class Distinct {

	/**
	 * @param args
	 */
	
	static String configFilePath="";
	static String tableName="";
	static DBSystem dbsystem = new DBSystem();
	static Set<String> set[];
	
	
	public static int getAttributeNumber(String tableName,String Attribute) 
	{
		
		int number=0;
		try {
			System.out.println("tablename:"+tableName+"\tAttribute:"+Attribute);
			RandomAccessFile raf = new RandomAccessFile(QueryChecker.configpath, "r");
			String line;
			boolean flag=false;
			number = 0;
			while((line=raf.readLine())!=null)
			{
				if(flag == true)
				{
					String token[] = line.split(",");
					token[0]=token[0].trim();
					
					if(token[0].compareToIgnoreCase(Attribute)==0)
					{
						break;
					}
					number++;
				}
				
				if(line.compareToIgnoreCase(tableName)==0)
				{
					flag= true;
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
	
	
	
	public static int V(String tableName, String Attribute) 
	{
	//	Set<String> distinct = new HashSet<String>();
		
		dbsystem.readConfig(configFilePath);
		dbsystem.populateDBInfo();
		int attNumber=getAttributeNumber(tableName,Attribute);
		System.out.println("number="+attNumber);
		
		Map<String, Set<String>[]> result = new HashMap<String, Set<String>[]>();
	/*	for(Map.Entry<String, Set<String>[]> e : dbsystem.tableDistinctData.entrySet())
		{
			String p_key = e.getKey();
			if(!p_key.contains("countries"))
				continue;
			Set<String> p_value[] = e.getValue();
			int number = p_value.length;
			
			for(int i=0;i<1;i++)
			{

				TreeSet<String> sortedSet = new TreeSet<String>(p_value[i]);
				System.out.println("\n\nOUTPUT: \n");
				System.out.println(sortedSet);
			}
		}*/
		
		
		
		
		Set<String> distinct[]= dbsystem.tableDistinctData.get(tableName+".csv");
		
	//	set = distinct;
		
		int returnValue=distinct[attNumber].size();
		return returnValue; 
		
	}
	
	public static void main(String[] args) 
	{
		// TODO Auto-generated method stub
		String decision="";
		String newRecord="";
		
		
		configFilePath = args[0];
		QueryChecker.configpath = args[0];
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter the tablename:");
		tableName = sc.nextLine();
		System.out.println("Enter the attribute:");
		String Attribute = sc.nextLine();
		int result = V(tableName,Attribute);
		System.out.println("result="+result);
		System.out.println("Do you want to add some records:<y/n>");
		decision = sc.nextLine();
		
		while( decision.charAt(0) == 'y' )
		{
			System.out.println("Enter the tablename in which you want to add the record:");
			tableName = sc.nextLine();
			System.out.println("Enter the record:");
			newRecord = sc.nextLine();
			System.out.println("Enter the attribute:");
			Attribute = sc.nextLine();
			
			try {
				
				set= dbsystem.tableDistinctData.get(tableName+".csv");
				
			//	System.out.println("len="+set[0].size());
				
				
				
				dbsystem.insertRecord(tableName, newRecord);
				String token[] = newRecord.split(",");
				for(int i =0;i<token.length;i++)
				{
					set[i].add(token[i]);
				}
				dbsystem.tableDistinctData.put(tableName, set);
				int number = getAttributeNumber(tableName,Attribute);
				set = dbsystem.tableDistinctData.get(tableName);
				if(set.length < number)
				{
					result = V(tableName,Attribute);
					System.out.println("result="+result);
				}
				else
				{
					result = set[number].size();
					System.out.println("result="+result);
				}
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
			System.out.println("Do you want to add some records:<y/n>");
			decision = sc.nextLine();
			
		
		}	
		
	}

}
