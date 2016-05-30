package com.centit.support.database.ddl;

import com.centit.support.database.metadata.TableField;
import com.centit.support.database.metadata.TableInfo;

public interface DDLOperations {
	/**
	 * 创建表
	 * @param tableInfo
	 */
	public void createTable(TableInfo tableInfo);
	/**
	 * 删除表
	 * @param tableCode
	 */
	public void dropTable(String tableCode);	
	/**
	 * 添加列
	 * @param tableCode
	 * @param column
	 */
	public void addColumn(String tableCode, TableField column);
	/**
	 * 修改列定义 ，比如 修改 varchar 的长度
	 * @param tableCode
	 * @param column
	 */
	public void modifyColumn(String tableCode, TableField column);
	/**
	 * 删除列
	 * @param tableCode
	 * @param columnCode
	 */
	public void dropColumn(String tableCode, String columnCode);
	/**
	 * 重命名列
	 * @param tableCode
	 * @param columnCode
	 * @param newColumnCode
	 */
	public void renameColumn(String tableCode, String columnCode, String newColumnCode);
	/**
	 * 重构列，涉及到内容格式的转换，需要新建一个列，将旧列中的数据转换到新列中，然后在删除旧列
	 * @param tableCode
	 * @param columnCode
	 * @param column
	 */
	public void reconfigurationColumn(String tableCode, String columnCode, TableField column);
}
