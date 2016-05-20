package com.centit.test;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.DataSourceDescription;
import com.centit.support.database.DatabaseAccess;
import com.centit.support.database.DbcpConnect;
import com.centit.support.database.DbcpConnectPools;
import com.centit.support.database.QueryUtils;

public class TestJDBC {
	
	public  static void  main(String[] args)   { 	
	  DataSourceDescription dbc = new DataSourceDescription();	  
	  dbc.setConnUrl("jdbc:oracle:thin:@192.168.131.81:1521:orcl");
	  dbc.setUsername("fdemo2");
	  dbc.setPassword("fdemo2");
	  /* String sql = "select loginName,userName from f_userinfo " +
		        "where [:(creepforin)userCodes| usercode in (:userCodes)]";*/
	  
	  String sql = "select loginName,userName from f_userinfo " +
		        "where usercode in (:userCodes)";
	  /*QueryAndParams qp = QueryAndParams.createFromQueryAndNamedParams(sql,
			 QueryUtils.createSqlParamsMap("userCodes",new Object[]{"U0000041","U0001013"}));*/
	  try {
		DbcpConnect conn= DbcpConnectPools.getDbcpConnect(dbc);
		JSONArray ja = DatabaseAccess.findObjectsByNamedSqlAsJSON(conn, sql,  QueryUtils.createSqlParamsMap("userCodes",new Object[]{"U0000041","U0001013"}));
		conn.close();
		System.out.println(ja.toJSONString());
	} catch (Exception e) {
		e.printStackTrace();
	}
  }
}
