package test.hive;

import com.db.main.Selectquery;

public class QueryValidator {

	/**
	 * @param args
	 */

	
	public void queryType(String query,String configpath)
    {
        
		  if(query.length()<=6)
		  {
			  System.out.println("Query Invalid");
			  return;
		  }
          String validate=query.toLowerCase().substring(0, 6);
          if(validate.compareToIgnoreCase("create")==0)
          {
        	  createCommand(query,configpath);
          }
          else if(validate.compareToIgnoreCase("select")==0)
          {
        	  selectCommand(query,configpath);
          }
          else
          {
        	  System.out.println("Query Invalid");
          }
        

    }

    public void createCommand(String query,String configpath)
    {
    	
        /*
        Use any SQL parser to parse the input query. Check if the table doesn't exists
        and execute the query.
        The execution of the query creates two files : <tablename>.data and
        <tablename>.csv. An entry should be made in the system config file used
        in previous deliverable.
        Print the query tokens as specified at the end.
        **format for the file is given below
        */

    	Createtable ct = new Createtable();
    	ct.CreatetableMethod(query,configpath);

    }

    public void selectCommand(String query,String configpath)
    {
        /*
        Use any SQL parser to parse the input query. Perform all validations (table
        name, attributes, datatypes, operations). Print the query tokens as specified
        below.
        */
    	System.out.println("In Select:configpath"+configpath);
    	Selectquery sc = new Selectquery();
    	sc.callTutorial(query, configpath);
    
    	
    }
}
