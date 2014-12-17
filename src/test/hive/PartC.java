package test.hive;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import com.db.main.DBSystem;
import com.db.main.Distinct;

public class PartC 
{
	static int matrix[][];
	static int s[][];
	static int tableCounter1;
	static String[] tableNames1;
	//DBSystem db;
	static String configFilePath;
	static Vector<String> tablename;
	static DBSystem db = new DBSystem();
	
	public PartC(String configFilePath1)
	{
		configFilePath = configFilePath1;
		
		db.readConfig(QueryChecker.configpath);
		db.populateDBInfo();
	}
	
	
	public static void CostEstimation(String[] tableNames,int tableCounter)
	{
		matrix = new int[tableCounter + 1][tableCounter + 1];
		s = new int[tableCounter + 1][tableCounter + 1];
		tableCounter1 = tableCounter;
		tableNames1 = tableNames;
		
		
		tablename=new Vector<String>();
		for(int i=0;i<tableCounter;i++)
			tablename.add(tableNames[i]);
		
		Iterator it =tablename.iterator();
		while(it.hasNext())
		{
			System.out.println("------"+it.next());
		}
		
		int i, j, k, l, minCost, cost;
		for (k = 2; k <= tableCounter; k++)
		{
			for (i = 1, j = k; j <= tableCounter; i++, j++) 
			{
				if (j - i == 1)
				{
					matrix[i][j] = calcultateCost(tableNames[i-1],tableNames[j-1]);
					s[i][j]=i;
				}
				else 
				{
					// find minimum
					minCost = Integer.MAX_VALUE;
					for (l = i; l < j; l++) 
					{
						
						if(matrix[i][l]==0)
						{
							//System.out.println("executed1");
							int noOfRow1 = db.returnLastID(tableNames[i-1])+1;
							//System.out.println("number of rows:"+noOfRow1+"---tablename:"+tableNames[i-1]);
							
							//Constant.PAGETABLE.get(tableNameInQuery.get(i-1)).endid.lastElement()+1;
							System.out.println("------------------------->"+matrix[i][l]);
							System.out.println("------------------------->"+matrix[l + 1][j]);
							System.out.println("------------------------->"+noOfRow1);
							System.out.println("------------------------->"+matrix[l + 1][j]);
							System.out.println("------------------------->"+commonMaxDistinctValue(tablename, i-1, l-1, j-1));
							cost = matrix[i][l]
									+ matrix[l + 1][j]
									+ ((noOfRow1* matrix[l + 1][j]) /
											commonMaxDistinctValue(tablename, i-1, l-1, j-1));
							//System.out.println("temp cost:"+cost);
						}
						else if (matrix[l + 1][j]==0)
						{
							//System.out.println("executed2");
							int noOfRow1 = db.returnLastID(tableNames[l])+1;
							//System.out.println("number of rows:"+noOfRow1+"---tablename:"+tableNames[l]);
							System.out.println("------------------------->"+matrix[i][l]);
							System.out.println("------------------------->"+matrix[l + 1][j]);
							System.out.println("------------------------->"+matrix[i][l]);
							System.out.println("------------------------->"+noOfRow1);
							System.out.println("------------------------->"+commonMaxDistinctValue(tablename, i-1, l-1, j-1));
							cost = matrix[i][l]
									+ matrix[l + 1][j]
									+ ((matrix[i][l] * noOfRow1) /
											commonMaxDistinctValue(tablename, i-1, l-1, j-1));
							//System.out.println("temp cost:"+cost);
						}
						else
						{
							//System.out.println("executed3");
							cost = matrix[i][l]
								+ matrix[l + 1][j]
								+ ((matrix[i][l] * matrix[l + 1][j]) /
										commonMaxDistinctValue(tablename, i-1, l-1, j-1));
						}
						System.out.println("CMDV:"+commonMaxDistinctValue(tablename, i-1, l-1, j-1));
						
						if (cost < minCost)
							{
							matrix[i][j] = cost;
							minCost=cost;
							s[i][j]=l;
							}
					}
					
				}
			}
			
		}
		//printMatrix();
		System.out.println(printOptimalParens(1,tableCounter));
		System.out.println("Cost:"+matrix[1][tableCounter1]);
		printMatrix();
	}
	
	// for printing of matrices
		private static void printMatrix() 
		{
			System.out.println("Cost Matrix:");
			for (int i = 0; i <= tableCounter1 ; i++) 
			{
				for (int j = 0; j <= tableCounter1 ; j++)
					System.out.printf(matrix[i][j] + "\t");
				System.out.println();
			}

			System.out.println("Paranthesis Matrix:");
			for (int i = 0; i <= tableCounter1; i++) 
			{
				for (int j = 0; j <= tableCounter1; j++)
					System.out.printf(s[i][j] + "\t");
				System.out.println();
			}
		}

		// call this function from 1 to size
		private static String printOptimalParens(int i, int j) {
			//System.out.println("H");
			if (i == j)
				return " " + tableNames1[i-1]+" ";//return "A[" + i + "]";
			else
				return "(" + printOptimalParens(i, s[i][j])
						+ printOptimalParens(s[i][j] + 1, j) + ")";
		}

		// ----------------------------------------------------------------------------------------------------

		private static int commonMaxDistinctValue(Vector<String> tablename,
				int i, int l, int j) {
			
			
			HashMap<String, String> first = new HashMap<String, String>();
			HashMap<String, String> second = new HashMap<String, String>();


			
			for (int index = i; index <= l; index++) {
				Vector<String> str = new Vector<String>();
				str = db.returnColumnNames(tableNames1[index],configFilePath);//Constant.TABLEDETAIL.get(tableNameInQuery.get(index)).columnName;
				Iterator<String> it = str.iterator();
				while (it.hasNext())
					{
						first.put(it.next(), tablename.get(index));
					}
			}

			for (int index = l + 1; index <= j; index++) {
				Vector<String> str = new Vector<String>();
				str = db.returnColumnNames(tableNames1[index],configFilePath);
				Iterator<String> it = str.iterator();
				while (it.hasNext())
				{
					second.put(it.next(), tablename.get(index));
				}
			}

			
			
			String table1 = null, table2 = null, commonAttrib = null;

			Iterator it = first.entrySet().iterator();
			while (it.hasNext()) 
			{
				Map.Entry pairs = (Map.Entry) it.next();
				if (second.containsKey(pairs.getKey())) 
				{
					table1 = (String) pairs.getValue();
					table2 = second.get(pairs.getKey());
					commonAttrib = pairs.getKey().toString();
				}
			}

			System.out.println("table1:"+table1+"\ttable2:"+table2);
			int vNoOfRow1 = Distinct.V(table1, commonAttrib);
			int vNoOfRow2 = Distinct.V(table2, commonAttrib);

			return Math.max(vNoOfRow1, vNoOfRow2);
		}

		// -----------------------------------------------------------------------------

		private static int calcultateCost(String table1, String table2) {
			int noOfRow1 = db.returnLastID(table1)+1;
			int noOfRow2 = db.returnLastID(table2)+1;;
			
			//System.out.println("total tuble in table 1:"+noOfRow1);
			//System.out.println("total tuble in table 2:"+noOfRow2);
			
			String commonAttrib = findCommonAttribut(table1, table2);

			if (commonAttrib == null) // doubt r1*r2 or 0
				{
				//System.out.println("for 2 table.. thr is no common attribute");
				return noOfRow1 * noOfRow2;
				}

			//System.out.println("for 2 table.. CA:"+commonAttrib);
		
			
			int vNoOfRow1 = Distinct.V(table1, commonAttrib);
			int vNoOfRow2 = Distinct.V(table2, commonAttrib);

			return (noOfRow1 * noOfRow2) / Math.max(vNoOfRow1, vNoOfRow2);
		}

		// returns name, only first common name will be retured

		private static String findCommonAttribut(String table1, String table2) {
			Vector<String> columnname1 = db.returnColumnNames(table1,configFilePath); 
			Vector<String> columnname2 = db.returnColumnNames(table2,configFilePath);
			String name;

			Iterator<?> it = columnname1.iterator();
			while (it.hasNext()) {
				name = (String) it.next();
				if (columnname2.contains(name))
					return name;
			}

			return null;
		}

	
	
}
