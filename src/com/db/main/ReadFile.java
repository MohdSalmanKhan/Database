package com.db.main;

import java.util.LinkedList;
import java.util.*;

/**
 * Created by root on 24/1/14.
 */
public class ReadFile {

	public int pageSize;
	public int numOfPages;
    public String returnValue;
    public String pathForData;
    public String[] tableName = new String[1000];
    int flag = 0;
    public int numOfTables = 0;

    public LinkedList<String> ll;
    public Map<String , LinkedList<String>> tableAttributes = new HashMap< String , LinkedList<String>>();


    public void readFile(String str)
    {
        if(str.toLowerCase().contains("page_size"))
        {
            for (String retval: str.split(" ", 2))
            {

                    /* during first iteration retval will contain the String "pagesize"
                        during second iteration retval will contain the integer value of the pagesize
                     */
                returnValue = retval;
            }

            pageSize = Integer.parseInt(returnValue);

//            System.out.println(pageSize);

        }
        else if(str.toLowerCase().contains("num_pages"))
        {
            for (String retval: str.split(" ", 2))
            {

                    /* during first iteration retval will contain the String "num_pages"
                        during second iteration retval will contain the integer value of the number of pages
                     */
                returnValue = retval;
            }

            numOfPages = Integer.parseInt(returnValue);

  //          System.out.println(numOfPages);
        }
        else if(str.toLowerCase().contains("path_for_data"))
        {
            for (String retval: str.split(" ", 2))
            {

                    /* during first iteration retval will contain the String "path_for_data"
                        during second iteration retval will contain the integer value of the path for data
                     */
                returnValue = retval;
            }

            pathForData = returnValue;

    //        System.out.println(pathForData);
        }
        else if(str.toLowerCase().contains("begin"))
        {
                flag = 1;
                ll = new LinkedList<String>();

        }
        else {
            if (flag == 1) {

                if (str.toLowerCase().compareToIgnoreCase("end") != 0) {
                    ll.add(str);
                } else {
                	tableName[numOfTables++] = ll.get(0).toString()+".csv";
                	ll.remove(0);
                    //tableName[numOfTables++] = ll.remove(0).toString() + ".csv";
            //        System.out.println("names="+tableName[numOfTables-1]);
                    tableAttributes.put(tableName[numOfTables - 1], ll);
                    
                    flag = 0;
                }



            /*for (String retval: str.split(" ", 2))
            {

                    // during first iteration retval will contain the String "path_for_data"
                    //  during second iteration retval will contain the integer value of the path for data

                returnValue = retval;
            }

            tableName[numOfTables++] = returnValue;

            flag = 0;

            System.out.println(tableName[numOfTables-1]);
            */


            }
        }

    }

}
