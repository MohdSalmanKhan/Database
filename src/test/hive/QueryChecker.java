package test.hive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class QueryChecker {
	
	public static String configpath="";
	
	public static void main(String args[]) {
		try 
		{
			QueryValidator queryValidator = new QueryValidator();
			configpath = args[0];
			System.out.println(configpath);
	//		String input = args[1];
			
	//		String configpath = "/home/salman/Desktop/config.txt";
	//		String input = "Select * from countries where NAME like Albania ORDER BY NAME  ";
			
			 Scanner sc = new Scanner(System.in);
		     int testCases = sc.nextInt();
		     String query="";
	//	     System.out.println("testcases="+testCases);
			for(int i=0; i<testCases ;i++)
			{
				Scanner sc1 = new Scanner(System.in); 
				query = sc1.nextLine();
	//			query ="select * from student1 join student2 on student1.B = student2.B join student3 on student2.C= student3.C ";//"select airports.ID , countries.ID , airports.NAME , countries.NAME ,REGION , CODE , CONTINENT from airports Join countries on countries.ID = airports.ID "; //"select airports.ID , countries.ID , airports.NAME , countries.NAME ,REGION , CODE , CONTINENT from airports Join countries on countries.ID = airports.ID ";//"select * from student1 join student2 on student1.B = student2.B join student3 on student2.C= student3.C ";//"select airports.ID , countries.ID , airports.NAME , countries.NAME ,REGION , CODE , CONTINENT from airports Join countries on countries.ID = airports.ID ";
						
	//			System.out.println("query="+query);
				queryValidator.queryType(query,configpath);
			
	//		File file = new File(input);
	//		FileReader fr = new FileReader(file);
	//		BufferedReader br = new BufferedReader(fr);

				
//				while ((query = br.readLine()) != null) {}
			//	queryValidator.queryType(query,configpath);
	//		}
			}
		}
		catch(Exception e)
		{
			//System.out.println("1");
			e.printStackTrace();
		}
	}
}
