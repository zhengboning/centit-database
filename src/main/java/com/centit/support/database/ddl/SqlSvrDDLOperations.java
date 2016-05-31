package com.centit.support.database.ddl;

import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

import com.centit.support.database.DBConnect;
import com.centit.support.database.DatabaseAccess;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;

public class SqlSvrDDLOperations implements DDLOperations {

	private DBConnect conn;
	public SqlSvrDDLOperations(){
		
	}
	
	public SqlSvrDDLOperations(DBConnect conn) {
		this.conn = conn;
	}
	
	public void setConnect(DBConnect conn) {
		this.conn = conn;
	}
	
	@Override
	public void createTable(TableInfo tableInfo) throws SQLException {
		// TODO Auto-generated method stub
		StringBuilder sbCreate = new StringBuilder("create table ");
		sbCreate.append(tableInfo.getTableName()).append(" (");
		for(TableField field : tableInfo.getColumns()){
			sbCreate.append(field.getColumnName())
				.append(" ").append(field.getColumnType());
			if(field.isMandatory())
				sbCreate.append(" not null");
			if(StringUtils.isNotBlank(field.getDefaultValue()))
				sbCreate.append(" default ").append(field.getDefaultValue());
			sbCreate.append(",");
		}
		sbCreate.append(" primary key (");
		int i=0;
		for(String pkfield : tableInfo.getPkColumns()){
			if(i>0)
				sbCreate.append(", ");
			sbCreate.append(pkfield);
			i++;
		}
		sbCreate.append(" ));");
		
		DatabaseAccess.doExecuteSql(conn, sbCreate.toString());
	}

	@Override
	public void dropTable(String tableCode) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addColumn(String tableCode, TableField column) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void modifyColumn(String tableCode, TableField column) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void dropColumn(String tableCode, String columnCode) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void renameColumn(String tableCode, String columnCode, String newColumnCode) throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reconfigurationColumn(String tableCode, String columnCode, TableField column) throws SQLException {
		// TODO Auto-generated method stub

	}

}
