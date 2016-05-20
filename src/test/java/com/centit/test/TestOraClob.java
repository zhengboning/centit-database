package com.centit.test;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.DataSourceDescription;
import com.centit.support.database.DatabaseAccess;
import com.centit.support.database.DbcpConnect;
import com.centit.support.database.DbcpConnectPools;

public class TestOraClob {
	
  public  static void  main(String[] args)   { 
	  testFetchClob();
  }
  public  static void testFetchClob(){
	  DataSourceDescription dbc = new DataSourceDescription();	  
	  dbc.setConnUrl("jdbc:oracle:thin:@192.168.131.81:1521:orcl");
	  dbc.setUsername("fdemo2");
	  dbc.setPassword("fdemo2");
	  
	  try {
		DbcpConnect conn= DbcpConnectPools.getDbcpConnect(dbc);
		String sSql = 
		"select T.VC_ID as id, T.VC_DUETYPE as vcDuetype, T.VC_OPINION as vcOpinion "+
        "from WP_REQUEST_DEP T "+
	    "where T.VC_ID in (select min(B.VC_ID) "+
                    " FROM WP_REQUEST A "+
                    "INNER JOIN WP_REQUEST_DEP B "+
                       "ON B.VC_REQUEST_ID = A.VC_REQUEST_ID "+
                    "WHERE A.VC_PROJECT_ID = '402808ec535fda5a0153600660950004' "+
                    "group by B.VC_DUETYPE)";
		JSONArray ja = DatabaseAccess.findObjectsAsJSON(conn, sSql,null,null);
		conn.close();
		System.out.println(ja.toJSONString());
	} catch (Exception e) {
		e.printStackTrace();
	}
	  
   }
  
  public  static void testCentitLob(){
	  DataSourceDescription dbc = new DataSourceDescription();
	  
	  dbc.setConnUrl("jdbc:oracle:thin:@192.168.131.81:1521:orcl");
	  dbc.setUsername("fdemo2");
	  dbc.setPassword("fdemo2");
	  
	  try {
		DbcpConnect conn= DbcpConnectPools.getDbcpConnect(dbc);
		PreparedStatement pStmt= conn.prepareStatement(
		"select NO, internal_no,item_id,STUFF , length(stuff),CENTIT_LOB.ClobToBlob(stuff) as bstuff " +
        "from inf_apply where  no='JS000000HD0000000481' ");
		ResultSet rs = pStmt.executeQuery();
		if (rs.next()) {
			Clob stuff = rs.getClob("STUFF");
			Blob bstuff = rs.getBlob("bstuff");
			String internal_no = rs.getString("internal_no");
			String item_id = rs.getString("item_id");
			PreparedStatement pStmt2= conn.prepareStatement(
					"begin DataTranslate.InsertAnnex(?,?,?); end;");
			
			pStmt2.setClob(1, stuff);
			pStmt2.setString(2, internal_no);
			pStmt2.setString(3, item_id);
			
			pStmt2.execute();
			pStmt2.close();
			
			System.out.println("Clob len :" + stuff.length());//12560267
			System.out.println("Blob len :" + bstuff.length());
		}
		pStmt.close();
		conn.close();
	} catch (Exception e) {
		e.printStackTrace();
	}
	  
   }
	
}
