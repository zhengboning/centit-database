package com.centit.support.database.metadata;

import com.centit.support.database.DBConnect;

public interface Database {
	public void setDBConfig(DBConnect dbc);
	public TableMetadata getTableMetadata(String tabName);
	public String getDBSchema() ;
	public void setDBSchema(String schema) ;
	
}
