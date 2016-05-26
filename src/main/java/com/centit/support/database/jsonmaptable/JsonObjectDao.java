package com.centit.support.database.jsonmaptable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.centit.support.database.metadata.TableInfo;

public interface JsonObjectDao {
	
	public TableInfo getTableInfo();	
	/**
	 * 单主键表
	 * @param keyValue
	 * @return
	 */
	public JSONObject getObjectById(Object keyValue) throws SQLException, IOException;
	/**
	 * 联合主键表
	 * @param keyValue
	 * @return
	 */
	public JSONObject getObjectById(Map<String,Object> keyValues) throws SQLException, IOException;
	
	/**
	 * 根据属性查询对象
	 * @param properties
	 * @return
	 */
	public JSONObject getObjectByProperties(Map<String, Object> properties) throws SQLException, IOException;
	
	/**
	 * 根据属性进行查询
	 * @param properties
	 * @param startPos
	 * @param maxSize
	 * @return
	 */
	public JSONArray listObjectsByProperties(Map<String, Object> properties,
			int startPos, int maxSize) throws SQLException, IOException;
	
	/**
	 * 保存
	 * @param object
	 */
	public void saveNewObject(JSONObject object) throws SQLException;
	
	/**
	 * 更改部分属性
	 * @param object
	 */
	public void updateObject(JSONObject object) throws SQLException;
	
	/**
	 * 合并
	 * @param object
	 */
	public void mergeObject(JSONObject object) throws SQLException;
	
	/**
	 * 删除，单主键
	 * @param keyValue
	 */
	public void deleteObjectById(Object keyValue) throws SQLException;
	
	/**
	 * 删除，联合主键
	 * @param keyValue
	 */
	public void deleteObjectById(Map<String,Object> keyValue) throws SQLException;
	
	//--- 作为子表批量操作
	/**
	 * 批量添加多条记录
	 * @param object
	 */
	public void insertObjectsAsTabulation(JSONArray objects) throws SQLException;
	/**
	 * 批量删除
	 * @param objects
	 */
	public void deleteObjectsAsTabulation(JSONArray objects) throws SQLException;
	
	/**
	 * 根据外键批量删除，单外键
	 * @param propertyName
	 * @param propertyValue
	 */
	public void deleteObjectsAsTabulation(final String propertyName,
            final Object propertyValue) throws SQLException;
	/**
	 * 根据外键批量删除，符合外键	 
	 * @param properties
	 */
	public void deleteObjectsAsTabulation(Map<String, Object> properties) throws SQLException;
	
	/**
	 * 用新的列表覆盖数据库中的列表
	 * @param dbObjects
	 * @param newObjects
	 * @return
	 */
	public void replaceObjectsAsTabulation(JSONArray newObjects,JSONArray dbObjects) throws SQLException, IOException;
	/**
	 * 用新的列表覆盖数据库中的内容，通过单外键查询列表
	 * @param newObjects
	 * @param propertyName
	 * @param propertyValue
	 */
	public void replaceObjectsAsTabulation(JSONArray newObjects,
    		final String propertyName,
            final Object propertyValue) throws SQLException, IOException;
	/**
	 * 用新的列表覆盖数据库中的内容，通过复合外键查询列表
	 * @param newObjects
	 * @param propertyName
	 * @param propertyValue
	 */
	public void replaceObjectsAsTabulation(JSONArray newObjects,
			Map<String, Object> properties) throws SQLException, IOException;
}
