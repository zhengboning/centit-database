package com.centit.support.database.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.centit.support.database.DBConnect;

public class MetadataPersistent {
	/**
	 * 这个方法暂时不需要实现，系统反向工程不需要通过元数据表来实现
	 * @param tabName
	 * @return
	 */
	private DBConnect dbc;
	public TableInfo loadTableMetadata(String tabName)
	{
		return null;
	}
	
	public void setDBConfig(DBConnect dbc){
		this.dbc=dbc;
	}
	/**
	 * 这个函数将覆盖系统中已经有的元数据记录，实际上他是先删除已有额元数据然后插入新的数据
	 * @param md 元数据记录，它最好是从PdmReader.getTableMetadata 返回的内容，
	 * 						如果是从数据库的系统视图中获得的元数据内容可能不完整
	 */
	public void saveTableMetadata(TableInfo md) {
		String sTabCode = md.getTabName().toUpperCase();
		PreparedStatement pStmt= null;
		try {
			Connection conn = dbc.getConn();
			//删除现有数据库中的元数据
			pStmt= conn.prepareStatement("delete from F_MD_TABLE where TBCODE=?");
			pStmt.setString(1, sTabCode);
			pStmt.executeUpdate();
			pStmt.close();
			pStmt= conn.prepareStatement("delete from F_MD_COLUMN where TBCODE=?");
			pStmt.setString(1, sTabCode);
			pStmt.executeUpdate();
			pStmt.close();
			pStmt= conn.prepareStatement("delete from F_MD_REL_DETIAL where RELCODE in (select RELCODE from F_MD_RELATION where PTABCODE=?)");
			pStmt.setString(1, sTabCode);
			pStmt.executeUpdate();
			pStmt.close();
			pStmt= conn.prepareStatement("delete from F_MD_RELATION where PTABCODE=?");
			pStmt.setString(1, sTabCode);
			pStmt.executeUpdate();
			pStmt.close();
			//保存表基本信息
			pStmt= conn.prepareStatement("insert into F_MD_TABLE(TBCODE,TBNAME,TBTYPE,TBSTATE,TBDESC,IsInWorkflow) values(?,?,'T','S',?,'F')");
			pStmt.setString(1, sTabCode);
			pStmt.setString(2, md.getTabDesc());
			pStmt.setString(3, md.getTabComment());
			pStmt.executeUpdate();
			pStmt.close();
			//保存表字段信息
			if(md.getColumns() !=null){
				pStmt= conn.prepareStatement("insert into F_MD_COLUMN(TBCODE,COLCODE,COLNAME,COLTYPE,ACCETYPE,COLLENGTH,COLPRECISION,COLSTATE,COLDESC,COLORDER) "+
								"values(?,?,?,?,'A',?,?,'T',?,?)");
				pStmt.setString(1, sTabCode);
				int nOrder=1;
				for(TableField col : md.getColumns()){
					pStmt.setString(2, col.getColumn().toUpperCase() );
					pStmt.setString(3, col.getDesc());
					pStmt.setString(4, col.getDBType());
					pStmt.setInt(5, col.getMaxLength()>col.getPrecision()?col.getMaxLength():col.getPrecision());
					pStmt.setInt(6, col.getScale());
					pStmt.setString(7, col.getComment());
					pStmt.setInt(8,nOrder++);
					pStmt.executeUpdate();
				}
				pStmt.close();
			}
			//保存表作为主表（父表）的关联信息
			if(md.getReferences() !=null){
				for(TableReference ref : md.getReferences()){
					//insert into F_MD_RELATION (RELCODE, RELNAME, PTABCODE, CTABCODE, RELSTATE, REFDESC) values 
					try{
						pStmt= conn.prepareStatement("insert into F_MD_RELATION (RELCODE, RELNAME, PTABCODE, CTABCODE, RELSTATE, REFDESC) "+
							"values(?,?,?,?,'T',?)");
						pStmt.setString(1, ref.getReferenceCode().toUpperCase());
						pStmt.setString(2, ref.getReferenceName() );
						pStmt.setString(3, md.getTabName().toUpperCase());
						pStmt.setString(4, ref.getTableName().toUpperCase());
						pStmt.setString(5, "外键连接");
						pStmt.executeUpdate();
						pStmt.close();
					
					
						pStmt= conn.prepareStatement("insert into F_MD_REL_DETIAL (RELCODE, PCOLCODE, CCOLCODE) "+
							"values(?,?,?)");
						pStmt.setString(1, ref.getReferenceCode().toUpperCase());
						int i=0;
						for(TableField col: ref.getFkcolumns()){
							//md.getPkColumns()
							pStmt.setString(2, md.getPkColumns().get(i).toUpperCase());
							pStmt.setString(3, col.getColumn().toUpperCase());
							i++;
							pStmt.executeUpdate();
						}
						pStmt.close();
					}catch(Exception e){
						pStmt.close();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			try{
				if(pStmt!=null)
					pStmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
	}
}
