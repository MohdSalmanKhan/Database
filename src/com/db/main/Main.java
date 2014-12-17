package com.db.main;

import test.hive.*;
import java.io.UnsupportedEncodingException;

/**
 * Created by root on 24/1/14.
 */
public class Main {

    public static void main(String[] args) throws UnsupportedEncodingException {
        //System.out.println("hi");

        try {
        	testCreateTable test = new testCreateTable();
        	test.test1();
        	
        	
        	
	/*		DBSystem dbSystem = new DBSystem();
      // dbSystem.readConfig("/tmp/config.txt");
			dbSystem.readConfig("/home/salman/workspace/Database/src/com/db/main/config.txt");
			dbSystem.populateDBInfo();


    //   System.out.println("#Pges : " + dbSystem.rf.numOfPages);
     //  System.out.println("Pges Size : " + dbSystem.rf.pageSize);


			dbSystem.getRecord("countries", 0);
			dbSystem.getRecord("countries", 1);
			dbSystem.getRecord("countries", 2);
			dbSystem.getRecord("countries", 1);
			dbSystem.getRecord("countries", 2);
			dbSystem.getRecord("countries", 2);
			dbSystem.getRecord("countries", 3);
			dbSystem.getRecord("countries", 41);
			dbSystem.getRecord("countries", 9);
			dbSystem.getRecord("countries", 39);
			dbSystem.getRecord("countries", 28);
			dbSystem.getRecord("countries", 1);
			dbSystem.getRecord("countries", 30);
			dbSystem.getRecord("countries", 38);
			dbSystem.getRecord("countries", 39);
			dbSystem.getRecord("countries", 31);
			dbSystem.insertRecord("countries", "record");

			dbSystem.getRecord("countries", 42);
			dbSystem.getRecord("countries", 28);*/
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

}