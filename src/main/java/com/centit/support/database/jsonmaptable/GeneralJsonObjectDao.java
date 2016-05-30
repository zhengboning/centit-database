package com.centit.support.database.jsonmaptable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.support.algorithm.ListOpt;
import com.centit.support.algorithm.NumberBaseOpt;
import com.centit.support.algorithm.ReflectionOpt;
import com.centit.support.algorithm.StringBaseOpt;
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
			sBuilder.append(col.getColumnName());
			fieldNames[i] = col.getPropertyName();
			i++;
		}
		return new ImmutablePair<String,String[]>(sBuilder.toString(),fieldNames);
	}
	
	public boolean isPkColumn(String propertyName){
		TableField field = tableInfo.findFieldByName(propertyName);
		return tableInfo.getPkColumns().contains(field.getColumnName());
	}
	
	
	public boolean checkHasAllPkColumns(Map<String, Object> properties){
		for(String pkc : tableInfo.getPkColumns() ){
			TableField field = tableInfo.findFieldByColumn(pkc);
			if( field != null &&
					properties.get(field.getPropertyName()) == null)
				return false;
		}
		return true;
	}
	
	public static String buildFilterSqlByPk(TableInfo ti,String alias){
		StringBuilder sBuilder= new StringBuilder();
		int i=0;
		for(String plCol : ti.getPkColumns()){
			if(i>0)
				sBuilder.append(" and ");
			TableField col = ti.findFieldByColumn(plCol);
			if(StringUtils.isNotBlank(alias))
				sBuilder.append(alias).append('.');
			sBuilder.append(col.getColumnName()).append(" = :").append(col.getPropertyName());
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
			sBuilder.append(col.getColumnName()).append(" = :").append(col.getPropertyName());
			i++;
		}
		return sBuilder.toString();
	}
	
	public static Pair<String,String[]> buildGetObjectSqlByPk(TableInfo ti){
		Pair<String,String[]> q = buildFieldSql(ti,null);
		String filter = buildFilterSqlByPk(ti,null);
		return new ImmutablePair<String,String[]>(
				"select " + q.getLeft() +" from " +ti.getTableName() + " where " + filter,
				q.getRight());
	}
	
	@Override
	public JSONObject getObjectById(Object keyValue) throws SQLException, IOException {
		if(tableInfo.getPkColumns()==null || tableInfo.getPkColumns().size()!=1)
			throw new SQLException("表"+tableInfo.getTableName()+"不是单主键表，这个方法不适用。");
		
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
		if(! checkHasAllPkColumns(keyValues)){
			throw new SQLException("缺少主键对应的属性。");
		}
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
				 "select " + q.getLeft() +" from " +tableInfo.getTableName() + " where " + filter,
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
				 "select " + q.getLeft() +" from " +tableInfo.getTableName() + " where " + filter,
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
				 "select " + q.getLeft() +" from " +tableInfo.getTableName() + " where " + filter,
				 properties,
				 q.getRight(),
				 startPos, maxSize);
	}

	private String buildInsertSql(Collection<String> fields){
		StringBuilder sbInsert = new StringBuilder("insert into ");
		sbInsert.append(tableInfo.getTableName()).append(" ( ");
		StringBuilder sbValues = new StringBuilder(" ) values ( ");
		int i=0;
		for(String f : fields){
			if(i>0){
				sbInsert.append(", ");
				sbValues.append(", ");
			}
			TableField col = tableInfo.findFieldByName(f);
			sbInsert.append(col.getColumnName());
			sbValues.append(":").append(f);
			i++;
		}
		return sbInsert.append(sbValues).append(")").toString();		
	}
	
	@Override
	public void saveNewObject( Map<String, Object> object) throws SQLException {
		/*if(! checkHasAllPkColumns(object)){
			throw new SQLException("缺少主键");
		}*/
		String sql = buildInsertSql(object.keySet());
		DatabaseAccess.doExecuteNamedSql(conn, sql, object);
	}

	private String buildUpdateSql(Collection<String> fields,final boolean exceptPk){
		StringBuilder sbUpdate = new StringBuilder("update ");
		sbUpdate.append(tableInfo.getTableName()).append(" set ");
		int i=0;
		for(String f : fields){
			if(exceptPk && isPkColumn(f))
				continue;
				
			if(i>0){
				sbUpdate.append(", ");
			}
			TableField col = tableInfo.findFieldByName(f);
			sbUpdate.append(col.getColumnName());
			sbUpdate.append(" = :").append(f);
			i++;
		}
		return sbUpdate.toString();		
	}
	
	@Override
	public void updateObject( Map<String, Object> object) throws SQLException {
		if(! checkHasAllPkColumns(object)){
			throw new SQLException("缺少主键对应的属性。");
		}
		String sql =  buildUpdateSql(object.keySet(),true) +
				" where " +  buildFilterSqlByPk(tableInfo,null);
		DatabaseAccess.doExecuteNamedSql(conn, sql, object);
	}

	@Override
	public void mergeObject( Map<String, Object> object) throws SQLException, IOException {
		if(! checkHasAllPkColumns(object)){
			throw new SQLException("缺少主键对应的属性。");
		}
		String sql =  
				"select count(1) as checkExists from " + tableInfo.getTableName() 
				+ " where " +  buildFilterSqlByPk(tableInfo,null);
		Long checkExists = NumberBaseOpt.castObjectToLong(
				DatabaseAccess.getScalarObjectQuery(conn, sql, object));
		if(checkExists==null || checkExists.equals(0)){
			saveNewObject(object);
		}else if(checkExists.equals(1)){
			updateObject(object);
		}else{
			throw new SQLException("主键属性有误，返回多个条记录。");
		}
	}

	@Override
	public void updateObjectsByProperties(Map<String, Object> fieldValues, Map<String, Object> properties)
			throws SQLException {
		String sql =  buildUpdateSql(fieldValues.keySet(),true) +
				" where " +  buildFilterSql(tableInfo,null,properties.keySet());
		Map<String, Object> paramMap = new HashMap<>();
		paramMap.putAll(fieldValues);
		paramMap.putAll(properties);
		DatabaseAccess.doExecuteNamedSql(conn, sql, paramMap);
	}
	
	@Override
	public void deleteObjectById(Object keyValue) throws SQLException {
		if(tableInfo.getPkColumns()==null || tableInfo.getPkColumns().size()!=1)
			throw new SQLException("表"+tableInfo.getTableName()+"不是单主键表，这个方法不适用。");
		
		String sql =  "delete from " + tableInfo.getTableName()+
				" where " +  buildFilterSqlByPk(tableInfo,null);
		DatabaseAccess.doExecuteNamedSql(conn, sql, 
				 QueryUtils.createSqlParamsMap( tableInfo.getPkColumns().get(0),keyValue) );
	}

	@Override
	public void deleteObjectById(Map<String, Object> keyValues) throws SQLException {
		if(! checkHasAllPkColumns(keyValues)){
			throw new SQLException("缺少主键对应的属性。");
		}
		String sql =  "delete from " + tableInfo.getTableName()+
				" where " +  buildFilterSqlByPk(tableInfo,null);
		DatabaseAccess.doExecuteNamedSql(conn, sql, keyValues );
	}
	
	@Override
	public void deleteObjectsByProperties(Map<String, Object> properties)
			throws SQLException {
		String sql =  "delete from " + tableInfo.getTableName()+
				" where " +  buildFilterSql(tableInfo,null,properties.keySet());
		DatabaseAccess.doExecuteNamedSql(conn, sql, properties );
		
	}

	@Override
	public void insertObjectsAsTabulation(JSONArray objects) throws SQLException {
		for(Object object : objects){
			saveNewObject((JSONObject)object);
		}
	}

	@Override
	public void deleteObjectsAsTabulation(JSONArray objects) throws SQLException {
		for(Object object : objects){
			deleteObjectById((JSONObject)object);
		}
	}

	@Override
	public void deleteObjectsAsTabulation(String propertyName, Object propertyValue) throws SQLException {
		deleteObjectsByProperties(QueryUtils.createSqlParamsMap(propertyName,propertyValue));
	}

	@Override
	public void deleteObjectsAsTabulation(Map<String, Object> properties) throws SQLException {
		deleteObjectsByProperties(properties);		
	}

	public class JSONObjectComparator implements Comparator<Object>{
		private TableInfo tableInfo;
		public JSONObjectComparator(TableInfo tableInfo){
			this.tableInfo = tableInfo;
		}
		@Override
		public int compare(Object o1, Object o2) {
			for(String pkc : tableInfo.getPkColumns() ){
				TableField field = tableInfo.findFieldByColumn(pkc);
				Object f1 = ((JSONObject) o1).get(field.getPropertyName());
				Object f2 = ((JSONObject) o2).get(field.getPropertyName());
				if(f1==null){
					if(f2!=null)
						return -1;
				}else{
					if(f2==null)
						return 1;
					if( ReflectionOpt.isNumberType(f1.getClass()) &&
							ReflectionOpt.isNumberType(f2.getClass())){
						double db1 = ((Number)f1).doubleValue();
						double db2 = ((Number)f2).doubleValue();
						if(db1>db2)
							return 1;
						if(db1<db2)
							return -1;
					}else{
						String s1 = StringBaseOpt.objectToString(f1);
						String s2 = StringBaseOpt.objectToString(f2);
						int nc = s1.compareTo(s2);
						if(nc!=0)
							return nc;
					}
				}
			}
			return 0;
		}
		
	}
	
	@Override
	public void replaceObjectsAsTabulation(JSONArray newObjects, JSONArray dbObjects)
			throws SQLException {
		//insert<T> update(old,new)<T,T> delete<T>
		Triple<List<Object>, List<Pair<Object,Object>>, List<Object>> 
		 comRes=
		 	ListOpt.compareTwoList(dbObjects, newObjects, new JSONObjectComparator(tableInfo));
		for(Object obj:comRes.getLeft()){
			saveNewObject((JSONObject)obj);
		}
		for(Object obj:comRes.getRight()){
			deleteObjectById((JSONObject)obj);
		}
		for(Pair<Object,Object> pobj:comRes.getMiddle()){
			updateObject((JSONObject)pobj.getRight());
		}
	}

	@Override
	public void replaceObjectsAsTabulation(JSONArray newObjects, String propertyName, Object propertyValue)
			throws SQLException, IOException {
		JSONArray dbObjects = listObjectsByProperties(
				QueryUtils.createSqlParamsMap(propertyName,propertyValue));
		replaceObjectsAsTabulation(newObjects,dbObjects);
	}

	@Override
	public void replaceObjectsAsTabulation(JSONArray newObjects, Map<String, Object> properties)
			throws SQLException, IOException  {
		JSONArray dbObjects = listObjectsByProperties(properties);
		replaceObjectsAsTabulation(newObjects,dbObjects);
	}
}
