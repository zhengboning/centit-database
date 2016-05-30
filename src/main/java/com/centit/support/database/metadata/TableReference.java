package com.centit.support.database.metadata;

import java.util.ArrayList;
import java.util.List;

public class TableReference {
	
	private List<TableField> fkcolumns;
	private String  sTableName;
	private String  sReferenceName;
	private String  sReferenceCode;
	private int nObjectId; //only used by sqlserver
	
	public int getObjectId() {
		return nObjectId;
	}
	public void setObjectId(int objectId) {
		nObjectId = objectId;
	}
	
	public List<TableField> getFkcolumns() {
		if(fkcolumns==null)
			fkcolumns = new ArrayList<TableField>();
		return fkcolumns;
	}
	public void setFkcolumns(List<TableField> fkcolumns) {
		this.fkcolumns = fkcolumns;
	}
	public String getTableName() {
		return sTableName;
	}
	public void setTableName(String tableName) {
		sTableName = tableName;
	}
	public String getReferenceName() {
		return sReferenceName;
	}
	public void setReferenceCode(String referenceCode) {
		sReferenceCode = referenceCode;
	}
	
	public String getReferenceCode() {
		return sReferenceCode;
	}
	public void setReferenceName(String referenceName) {
		sReferenceName = referenceName;
	}
	
	
	public boolean containColumn(String sCol) {
		if(sCol==null || fkcolumns==null || fkcolumns.size() == 0)
			return false;
		for(TableField tf : fkcolumns){
			if(sCol.equalsIgnoreCase(tf.getColumnName()))
				return true;
		}
		return false;
	}	
	
	public String getClassName() {
		String sClassName = TableField.mapPropName(sTableName);
		return sClassName.substring(0,1).toUpperCase() + 
				sClassName.substring(1);
	}
}
