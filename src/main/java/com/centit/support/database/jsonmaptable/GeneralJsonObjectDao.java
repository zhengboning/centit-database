package com.centit.support.database.jsonmaptable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.support.database.DBConnect;
import com.centit.support.database.DatabaseAccess;
import com.centit.support.database.QueryUtils;
import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;


public abstract class GeneralJsonObjectDao implements JsonObjectDao {
	
	private DBConnect conn;
	
	private TableInfo tableInfo;
	
	public GeneralJsonObjectDao(){
		
	}
	
	public GeneralJsonObjectDao(DBConnect conn,TableInfo tableInfo) {
		this.conn = conn;
		this.tableInfo = tableInfo;
	}
	
	public GeneralJsonObjectDao(TableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}
		
	public void setConnect(DBConnect conn) {
		this.conn = conn;
	}
	
	public DBConnect getConnect() {
		return this.conn;
	}
	public void setTableInfo(TableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}
	
	@Override
	public TableInfo getTableInfo() {
		return this.tableInfo;
	}

	/**
	 * 返回 sql 语句 和 属性名数组
	 * @param ti
	 * @return
	 */
	public static Pair<String,String[]> buildFieldSql(TableInfo ti,String alias){
		StringBuilder sBuilder= new StringBuilder();
		List<TableField> columns = ti.getColumns();
		String [] fieldNames = new String[columns.size()];
		boolean addAlias = StringUtils.isNotBlank(alias);
		int i=0;
		for(TableField col : columns){
			if(i>0)
				sBuilder.append(", ");
			else
				sBuilder.append(" ");
			if(addAlias)
				sBuilder.append(alias).append('.');
			sBuilder.append(col.getColumn());
			fieldNames[i] = col.getName();
			i++;
		}
		return new ImmutablePair<String,String[]>(sBuilder.toString(),fieldNames);
	}
	
	public static String buildFilterSqlByPk(TableInfo ti,String alias){
		StringBuilder sBuilder= new StringBuilder();
		int i=0;
		for(String plCol : ti.getPkColumns()){
			if(i>0)
				sBuilder.append(" and ");
			TableField col = ti.findField(plCol);
			if(StringUtils.isNotBlank(alias))
				sBuilder.append(alias).append('.');
			sBuilder.append(col.getColumn()).append(" = :").append(col.getName());
			i++;
		}
		return sBuilder.toString();
	}

	public static String buildFilterSql(TableInfo ti,String alias,Collection<String> properties){
		StringBuilder sBuilder= new StringBuilder();
		int i=0;
		for(String plCol : properties){
			if(i>0)
				sBuilder.append(" and ");
			TableField col = ti.findFieldByName(plCol);
			if(StringUtils.isNotBlank(alias))
				sBuilder.append(alias).append('.');
			sBuilder.append(col.getColumn()).append(" = :").append(col.getName());
			i++;
		}
		return sBuilder.toString();
	}
	
	public static Pair<String,String[]> buildGetObjectSqlByPk(TableInfo ti){
		Pair<String,String[]> q = buildFieldSql(ti,null);
		String filter = buildFilterSqlByPk(ti,null);
		return new ImmutablePair<String,String[]>(
				"select " + q.getLeft() +" from " +ti.getTabName() + " where " + filter,
				q.getRight());
	}
	
	@Override
	public JSONObject getObjectById(Object keyValue) throws SQLException, IOException {
		if(tableInfo.getPkColumns()==null || tableInfo.getPkColumns().size()!=1)
			return  null;
		Pair<String,String[]> q = buildGetObjectSqlByPk(tableInfo);
		JSONArray ja = DatabaseAccess.findObjectsByNamedSqlAsJSON(
				 conn, q.getLeft(), 
				 QueryUtils.createSqlParamsMap( tableInfo.getPkColumns().get(0),keyValue),
				 q.getRight());
		if(ja.size()<1)
			return null;
		return (JSONObject) ja.get(0);
	}

	@Override
	public JSONObject getObjectById(Map<String, Object> keyValues) throws SQLException, IOException {
		if(tableInfo.getPkColumns()==null || tableInfo.getPkColumns().size()!=keyValues.size())
			return  null;
		Pair<String,String[]> q = buildGetObjectSqlByPk(tableInfo);
		JSONArray ja = DatabaseAccess.findObjectsByNamedSqlAsJSON(
				 conn, q.getLeft(), 
				 keyValues,
				 q.getRight());
		if(ja.size()<1)
			return null;
		return (JSONObject) ja.get(0);
	}

	@Override
	public JSONObject getObjectByProperties(Map<String, Object> properties) throws SQLException, IOException {
		
		Pair<String,String[]> q = buildFieldSql(tableInfo,null);
		String filter = buildFilterSql(tableInfo,null,properties.keySet());
		JSONArray ja = DatabaseAccess.findObjectsByNamedSqlAsJSON(
				 conn,
				 "select " + q.getLeft() +" from " +tableInfo.getTabName() + " where " + filter,
				 properties,
				 q.getRight());
		if(ja.size()<1)
			return null;
		return (JSONObject) ja.get(0);
	}

	@Override
	public JSONArray listObjectsByProperties(Map<String, Object> properties) throws SQLException, IOException {
		Pair<String,String[]> q = buildFieldSql(tableInfo,null);
		String filter = buildFilterSql(tableInfo,null,properties.keySet());
		return DatabaseAccess.findObjectsByNamedSqlAsJSON(
				 conn,
				 "select " + q.getLeft() +" from " +tableInfo.getTabName() + " where " + filter,
				 properties,
				 q.getRight());
	}
	
	@Override
	public JSONArray listObjectsByProperties(Map<String, Object> properties, int startPos, int maxSize)
			throws SQLException, IOException {
		Pair<String,String[]> q = buildFieldSql(tableInfo,null);
		String filter = buildFilterSql(tableInfo,null,properties.keySet());
		return DatabaseAccess.findObjectsByNamedSqlAsJSON(
				 conn,
				 "select " + q.getLeft() +" from " +tableInfo.getTabName() + " where " + filter,
				 properties,
				 q.getRight(),
				 startPos, maxSize);
	}

	@Override
	public void saveNewObject(JSONObject object) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateObject(JSONObject object) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mergeObject(JSONObject object) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateObjectsByProperties(Map<String, Object> fieldValues, Map<String, Object> properties)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void deleteObjectById(Object keyValue) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteObjectById(Map<String, Object> keyValue) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertObjectsAsTabulation(JSONArray objects) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteObjectsAsTabulation(JSONArray objects) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteObjectsAsTabulation(String propertyName, Object propertyValue) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteObjectsAsTabulation(Map<String, Object> properties) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void replaceObjectsAsTabulation(JSONArray newObjects, JSONArray dbObjects) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void replaceObjectsAsTabulation(JSONArray newObjects, String propertyName, Object propertyValue)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void replaceObjectsAsTabulation(JSONArray newObjects, Map<String, Object> properties) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	

}
