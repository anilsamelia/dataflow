package com.eviden.ekart;

import java.sql.*;  
/*
 * 
 
#host sql.freedb.tech
#DB: freedb_amx-db
#User:freedb_amxroot
#Password:!F9Kb5z$zaZBSMU

 
 
 
 * */

public class DBConnectionTEst {
	
	public static void main(String args[]){  
		try{  
		Class.forName("com.mysql.jdbc.Driver");  
		Connection con=DriverManager.getConnection(  
		"jdbc:mysql://sql.freedb.tech:3306/freedb_amx-db","freedb_amxroot","!F9Kb5z$zaZBSMU");  
		//here sonoo is database name, root is username and password  
		System.out.println(con);  
		 
		con.close();  
		}catch(Exception e){ System.out.println(e);}  
		}  

}
