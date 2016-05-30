package com.centit.support.database.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;

import com.centit.support.database.DBConnect;

public class IbmDb2Metadata implements DatabaseMetadata {
	

	private final static String sqlGetTabColumns=
		"select a.name,a.coltype,a.length, a.scale, a.nulls "+
		"from sysibm.systables b , sysibm.syscolumns a "+
		"where a.tbcreator= ? and a.tbname= ? "+
		      "and b.name=a.tbname and b.creator=a.tbcreator";
	
	private final static String sqlPKInfo=
		"select constname, colname "+
		"from sysibm.syskeycoluse "+
		"where tbcreator=? and tbname=? "+ 
		"order by colseq";

	private final static String sqlFKInfo=
		"select tbname, relname, colcount, fkcolnames, pkcolnames "+
		"from sysibm.sysrels "+
		"where refkeyname= ?";
	
	private final static String sqlFKColumn=
		"select a.name,a.coltype,a.length, a.scale, a.nulls "+
		"from sysibm.systables b , sysibm.syscolumns a "+
		"where a.tbcreator= ? and a.tbname= ? and a.name= ? "+
		      "and b.name=a.tbname and b.creator=a.tbcreator";
	
	private String sDBSchema ;

	private DBConnect dbc;
	
	@Override
	public void setDBConfig(DBConnect dbc){
		this.dbc=dbc;
	}	
	public String getDBSchema() {
		return sDBSchema;
	}

	public void setDBSchema(String schema) {
		if(schema !=null)
			sDBSchema = schema.toUpperCase();
	}

	public TableInfo getTableMetadata(String tabName) {
		TableInfo tab = new TableInfo(tabName);
		PreparedStatement pStmt= null;
		ResultSet rs = null;
		try {
			Connection conn = dbc.getConn();
			tab.setSchema( dbc.getSchema().toUpperCase());
			// get columns
			pStmt= conn.prepareStatement(sqlGetTabColumns);
			pStmt.setString(1, sDBSchema);
			pStmt.setString(2, tabName);
			rs = pStmt.executeQuery();
			while (rs.next()) {
				TableField field = new TableField();
				field.setColumnName(rs.getString("name"));
				field.setColumnType(rs.getString("coltype"));
				field.setMaxLength(rs.getInt("length"));
				field.setPrecision(field.getMaxLength());
				field.setScale(rs.getInt("scale"));
				field.setNullEnable(rs.getString("nulls"));
				field.mapToMetadata();
		
				tab.getColumns().add(field);
			}
			rs.close();
			pStmt.close();
			// get primary key
			pStmt= conn.prepareStatement(sqlPKInfo);
			pStmt.setString(1, sDBSchema);
			pStmt.setString(2, tabName);
			rs = pStmt.executeQuery();
			while (rs.next()) {
				tab.setPkName(rs.getString("constname"));
				tab.getPkColumns().add(rs.getString("colname"));
			}
			rs.close();
			pStmt.close();
			// get reference info 
	 
			pStmt= conn.prepareStatement(sqlFKInfo);
			pStmt.setString(1, tab.getPkName());
			rs = pStmt.executeQuery();
			while (rs.next()) {
				TableReference ref = new TableReference();
				ref.setTableName(rs.getString("tbname"));
				ref.setReferenceCode(rs.getString("relname"));
				int nColCount = rs.getInt("colcount");
				String sFColNames = rs.getString("fkcolnames").trim();
				String [] p = sFColNames.split("\\s+");
				if(nColCount != p.length){
					System.out.println("外键"+ref.getReferenceCode()+"字段分隔出错！");
				}
				for(int i=0;i<p.length;i++){
					TableField field = new TableField();
					field.setColumnName(p[i]);
					ref.getFkcolumns().add(field);					
				}
				tab.getReferences().add(ref );
			}
			rs.close();
			pStmt.close();
			// get reference detail
			for(Iterator<TableReference> it= tab.getReferences().iterator();it.hasNext(); ){
				TableReference ref = it.next();
				for(Iterator<TableField> it2= ref.getFkcolumns().iterator();it2.hasNext(); ){
					TableField field = it2.next();
					pStmt= conn.prepareStatement(sqlFKColumn);
					pStmt.setString(1,sDBSchema);
					pStmt.setString(2,ref.getTableName());
					pStmt.setString(3,field.getColumnName());
					rs = pStmt.executeQuery();
					if (rs.next()) {
						field.setColumnType(rs.getString("coltype"));
						field.setMaxLength(rs.getInt("length"));
						field.setPrecision(field.getMaxLength());
						field.setScale(rs.getInt("scale"));
						field.setNullEnable(rs.getString("nulls"));
						field.mapToMetadata();
					}
					rs.close();
					pStmt.close();
				}
			}
			//conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try{
				if(pStmt!=null)
					pStmt.close();
				if(rs!=null)
					pStmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return tab;
	}

}
