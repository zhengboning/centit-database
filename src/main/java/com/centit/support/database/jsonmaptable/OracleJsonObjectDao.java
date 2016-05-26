package com.centit.support.database.jsonmaptable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.alibaba.fastjson.JSONArray;
import com.centit.support.database.DBConnect;
import com.centit.support.database.DatabaseAccess;
import com.centit.support.database.QueryUtils;
import com.centit.support.database.metadata.TableInfo;

public class OracleJsonObjectDao extends GeneralJsonObjectDao {

	public OracleJsonObjectDao(){
		
	}
	
	public OracleJsonObjectDao(DBConnect conn,TableInfo tableInfo) {
		super(conn,tableInfo);
	}
	
	public OracleJsonObjectDao(TableInfo tableInfo) {
		super(tableInfo);
	}
	
	@Override
	public JSONArray listObjectsByProperties(Map<String, Object> properties, int startPos, int maxSize)
			throws SQLException, IOException {
		
		TableInfo tableInfo = this.getTableInfo();
		
		Pair<String,String[]> q = buildFieldSql(tableInfo,null);
		String filter = buildFilterSql(tableInfo,null,properties.keySet());
		return DatabaseAccess.findObjectsByNamedSqlAsJSON(
					getConnect(),
					QueryUtils.buildOracleLimitQuerySQL(
							"select " + q.getLeft() +" from " +tableInfo.getTabName() + " where " + filter,
							startPos, maxSize, false),
				 properties,
				 q.getRight());
	}
}
