package com.centit.support.database.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Iterator;

import com.centit.support.database.DBConnect;


public class MsSqlSvrMetadata implements DatabaseMetadata {
	private final static String sqlGetTabColumns=
		"SELECT  a.name, c.name AS typename, a.length , a.xprec, a.xscale, isnullable "+
		"FROM syscolumns a INNER JOIN "+
		      "sysobjects b ON a.id = b.id INNER JOIN "+
		      "systypes c ON a.xtype = c.xtype "+
		"WHERE b.xtype = 'U' and b.name = ? "+
		"ORDER BY a.colorder";
			
	private final static String sqlPKName=
		"select a.name,a.object_id, a.parent_object_id ,a.unique_index_id  "+
		"from sys.key_constraints a , sysobjects b " +
		"where a.type='PK' and " +
			" a.parent_object_id=b.id and b.xtype = 'U' and b.name = ? ";

	private final static String sqlPKColumns=
		"select a.name "+
		"from sys.index_columns b join sys.columns a on(a.object_id=b.object_id and a.column_id=b.column_id) "+
		"where b.object_id=? and b.index_id=? "+
		"order by b.key_ordinal";
	//两个参数 均是 integer 对应上面的 parent_object_id 和 unique_index_id


	//foreign_keys
	private final static String sqlFKNames=
		"select a.name,a.object_id,a.parent_object_id , b.name as tabname "+
		"from sys.foreign_keys a join sysobjects b ON a.parent_object_id = b.id "+
		"where referenced_object_id = ? ";
		//参数对应与上面的 parent_object_id 也就是 主表的ID

	//foreign_key_columns
	private final static String sqlFKColumns=
		"SELECT  a.name, c.name AS typename, a.length , a.xprec, a.xscale, isnullable "+
		"FROM syscolumns a INNER JOIN "+ 
		       "sys.foreign_key_columns b ON a.id = b.parent_object_id  and b.parent_column_id=a.colid JOIN "+
		      "systypes c ON a.xtype = c.xtype "+
		"WHERE b.constraint_object_id=? "+ 
		"ORDER BY b.constraint_column_id";
	//参数对应与上面的 object_id
	
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
		sDBSchema = schema;
	}
	
	public TableInfo getTableMetadata(String tabName) {
		TableInfo tab = new TableInfo(tabName);
		int table_id=0,pk_ind_id=0;
		PreparedStatement pStmt= null;
		ResultSet rs = null;
		try {
			Connection conn = dbc.getConn();
			tab.setSchema( dbc.getSchema().toUpperCase());
			// get columns
			pStmt= conn.prepareStatement(sqlGetTabColumns);
			pStmt.setString(1, tabName);
			rs = pStmt.executeQuery();
			while (rs.next()) {
				// a.name, c.name AS typename, a.length , a.xprec, a.xscale, isnullable
				TableField field = new TableField();
				field.setColumnName(rs.getString("name"));
				field.setColumnType(rs.getString("typename"));
				field.setMaxLength(rs.getInt("length"));
				field.setPrecision(rs.getInt("xprec"));
				field.setScale(rs.getInt("xscale"));
				field.setNullEnable(rs.getString("isnullable"));
				field.mapToMetadata();
				
				tab.getColumns().add(field);
			}
			rs.close();
			pStmt.close();
			
			// get primary key
			pStmt= conn.prepareStatement(sqlPKName);
			pStmt.setString(1, tabName);
			rs = pStmt.executeQuery();
			if (rs.next()) {
				tab.setPkName(rs.getString("name"));
				table_id = rs.getInt("parent_object_id");
				//pk_id = rs.getInt("object_id");
				pk_ind_id = rs.getInt("unique_index_id");
			}
			rs.close();
			pStmt.close();
			
			pStmt= conn.prepareStatement(sqlPKColumns);
			pStmt.setInt(1, table_id);
			pStmt.setInt(2, pk_ind_id);
			rs = pStmt.executeQuery();
			while (rs.next()) {
				tab.getPkColumns().add(rs.getString("name"));
			}
			rs.close();
			pStmt.close();
			// get reference info 
			 
			pStmt= conn.prepareStatement(sqlFKNames);
			pStmt.setInt(1, table_id);
			rs = pStmt.executeQuery();
			while (rs.next()) {
				TableReference ref = new TableReference();
				//"select a.name,a.object_id,a.parent_object_id , b.name as tabname "+
				ref.setTableName(rs.getString("tabname"));
				ref.setReferenceCode(rs.getString("name"));
				ref.setObjectId( rs.getInt("object_id" ));
				tab.getReferences().add(ref );
			}
			rs.close();
			pStmt.close();
			// get reference detail
			for(Iterator<TableReference> it= tab.getReferences().iterator();it.hasNext(); ){
				TableReference ref = it.next();
				pStmt= conn.prepareStatement(sqlFKColumns);
				pStmt.setInt(1,ref.getObjectId());
				rs = pStmt.executeQuery();
				while (rs.next()) {
					//"select a.name,a.object_id,a.parent_object_id , b.name as tabname "+
					TableField field = new TableField();
					field.setColumnName(rs.getString("name"));
					field.setColumnType(rs.getString("typename"));
					field.setMaxLength(rs.getInt("length"));
					field.setPrecision(rs.getInt("xprec"));
					field.setScale(rs.getInt("xscale"));
					field.setNullEnable(rs.getString("isnullable"));
					field.mapToMetadata();
					
					ref.getFkcolumns().add(field);
				}
				rs.close();
				pStmt.close();
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
