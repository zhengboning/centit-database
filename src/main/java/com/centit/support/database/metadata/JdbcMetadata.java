package com.centit.support.database.metadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import com.centit.support.database.DBConnect;

public class JdbcMetadata implements DatabaseMetadata {

	private DBConnect dbc;

	@Override
	public void setDBConfig(DBConnect dbc) {
		this.dbc = dbc;
	}

	/**
	 * 没有获取外键
	 */
	@Override
	public TableInfo getTableMetadata(String tabName) {
		TableInfo tab = new TableInfo(tabName);
		try {
			tab.setSchema(dbc.getSchema().toUpperCase());
			DatabaseMetaData dbmd = dbc.getMetaData();
			
			ResultSet rs = dbmd.getTables(null, dbc.getSchema(), tabName, null);
			if(rs.next()) {
				tab.setTableLableName(rs.getString("REMARKS"));
			}
			rs.close();
			
			rs = dbmd.getTables(dbc.getCatalog(), dbc.getSchema(), tabName, null);
			while (rs.next()) {
				TableField field = new TableField();
				field.setColumnName(rs.getString("COLUMN_NAME"));
				field.setColumnType(rs.getString("TYPE_NAME"));
				field.setMaxLength(rs.getInt("COLUMN_SIZE"));
				field.setPrecision(rs.getInt("DECIMAL_DIGITS"));
				field.setScale(rs.getInt("COLUMN_SIZE"));
				field.setNullEnable(rs.getString("NULLABLE"));
				field.setColumnComment( rs.getString("REMARKS"));
				field.mapToMetadata();
				tab.getColumns().add(field);
			}
			rs.close();
			rs = dbmd.getPrimaryKeys(dbc.getCatalog(),dbc.getSchema(), tabName);
			while (rs.next()) {
				tab.getPkColumns().add(rs.getString("COLUMN_NAME"));
				tab.setPkName(rs.getString("PK_NAME"));
			}
			rs.close();
			
			dbmd.getExportedKeys(dbc.getCatalog(),dbc.getSchema(), tabName);
			Map<String , TableReference> refs = new HashMap<String , TableReference>();
			while (rs.next()) {
				String fkTableName = rs.getString("FKTABLE_NAME");
				TableReference ref= refs.get(fkTableName);
				if(ref==null){
					ref = new TableReference();
					ref.setTableName(fkTableName);
					ref.setReferenceCode(rs.getString("FK_NAME"));
				}
				TableField field = new TableField();
				field.setColumnName(rs.getString("FKCOLUMN_NAME"));
				ref.getFkcolumns().add(field);
			}
			rs.close();
			
			for(Map.Entry<String , TableReference> entry:refs.entrySet()){
				tab.getReferences().add(entry.getValue());
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return tab;
	}

	@Override
	public String getDBSchema() {		
		try {
			return dbc.getSchema();
		} catch (SQLException e) {
			return null;
		}
	}

	@Override
	public void setDBSchema(String schema) {
		
	}
}
