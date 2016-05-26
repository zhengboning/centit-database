package com.centit.support.database.metadata;

import com.centit.support.database.DBConnect;

public class JdbcMetadata implements DatabaseMetadata {

	private DBConnect dbc;
	
	@Override
	public void setDBConfig(DBConnect dbc){
		this.dbc=dbc;
	}	

	@Override
	public TableInfo getTableMetadata(String tabName) {
		// TODO  {"TABLE_NAME":"F_USERINFO","TABLE_TYPE":"TABLE","TABLE_SCHEM":"FDEMO2"}
		// {"TABLE_NAME":"F_USERINFO","CHAR_OCTET_LENGTH":32,"SQL_DATETIME_SUB":0,
		//  "TABLE_SCHEM":"FDEMO2","BUFFER_LENGTH":0,"NULLABLE":0,"IS_NULLABLE":"NO",
		//  "SQL_DATA_TYPE":0,"NUM_PREC_RADIX":10,"COLUMN_SIZE":32,"TYPE_NAME":"VARCHAR2",
		//  "IS_AUTOINCREMENT":"NO","COLUMN_NAME":"USERCODE","ORDINAL_POSITION":1,"DATA_TYPE":12}
		return null;
	}

	@Override
	public String getDBSchema() {
		if(dbc==null)
			return "";
		return dbc.getDbSchema();
	}

	@Override
	public void setDBSchema(String schema) {		
	}

}
