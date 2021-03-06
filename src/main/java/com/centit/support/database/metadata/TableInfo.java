package com.centit.support.database.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import com.centit.support.file.FileSystemOpt;

public class TableInfo {
	/**
	 * 包括主键
	 */
	private List<TableField> columns=null;
	/**
	 * 主键字段
	 */
	private List<String> pkColumns=null;
	private String schema;

	private String tableName;// 其实是table 代码 code
	//private String sClassName;//表对应的类名 同时作为业务模块名
	private String tableLabelName;// 表的 描述，中文名称
	private String tableComment;// 表的备注信息 
	private String pkName;
	private List<TableReference> references=null;
	private String packageName;
	
	/**
	 * 数据库表名，对应pdm中的code，对应元数据中的 tabcode
	 */
	public String getTableName() {
		return tableName;
		
	}
	
	public void setTableName(String tabName) {
		tableName = tabName;

	}	
	
	/**
	 * 数据库表中文名，对应pdm中的name,对应元数据中的 tabname
	 */
	public String getTableLableName() {
		return tableLabelName;
	}

	public void setTableLableName(String tabDesc) {
		this.tableLabelName = tabDesc;
	}

	/**
	 * 数据库表备注信息，对应pdm中的Comment,对应元数据中的 tabdesc
	 */
	public String getTableComment() {
		return tableComment;
	}

	public void setTableComment(String tabComment) {
		this.tableComment = tabComment;
	}

	public String getPkName() {
		return pkName;
	}

	public void setPkName(String pkName) {
		this.pkName = pkName;
	}	
	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}	
	protected static void writerXMLFile(Document doc, String xmlFile){
		XMLWriter output;
		try {
			output = new XMLWriter(
				      new FileWriter( new File(xmlFile) ));
		    output.write( doc );
		    output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * 根据字段名查找 字段信息
	 * @param colname
	 * @return
	 */
	public TableField findField(String colname){
		for(Iterator<TableField> it = columns.iterator();it.hasNext();){
			TableField col = it.next();
			if(col.getColumnName().equals(colname))
				return col;
		}
		return null;
	}
	
	/**
	 * 根据属性名查找 字段信息
	 * @param colname
	 * @return
	 */
	public TableField findFieldByName(String name){
		for(Iterator<TableField> it = columns.iterator();it.hasNext();){
			TableField col = it.next();
			if(col.getPropertyName().equals(name))
				return col;
		}
		for(Iterator<TableField> it = columns.iterator();it.hasNext();){
			TableField col = it.next();
			if(col.getColumnName().equals(name))
				return col;
		}
		return null;
	}
	
	/**
	 * 根据属性名查找 字段信息
	 * @param colname
	 * @return
	 */
	public TableField findFieldByColumn(String name){
		for(Iterator<TableField> it = columns.iterator();it.hasNext();){
			TableField col = it.next();
			if(col.getColumnName().equals(name))
				return col;
		}
		for(Iterator<TableField> it = columns.iterator();it.hasNext();){
			TableField col = it.next();
			if(col.getPropertyName().equals(name))
				return col;
		}
		return null;
	}
	
	public boolean isParmaryKey(String colname){
		if(pkColumns==null)
			return false;
		for(Iterator<String> it = pkColumns.iterator();it.hasNext();){
			String col = it.next();
			if(col.equals(colname))
				return true;
		}
		return false;
	}
	
	public TableInfo()
	{
		
	}
	
	public TableInfo(String tabname)
	{
		setTableName(tabname);
	}
	
	private void saveProperty(TableField field,Element propElt,boolean keyProp){
		propElt.addAttribute("name", field.getPropertyName());
		propElt.addAttribute("type", field.getHibernateType());
		Element colElt = propElt.addElement("column");
		saveColumn(field,colElt,keyProp);
	}
	
	private void saveColumn(TableField field,Element colElt,boolean keyProp){
		colElt.addAttribute("name", field.getColumnName().toUpperCase());
		if("Long".equals(field.getJavaType()) || "Double".equals(field.getJavaType()) ){
			colElt.addAttribute("precision", String.valueOf(field.getPrecision()));
			colElt.addAttribute("scale", String.valueOf(field.getScale()));
		}else if(field.getMaxLength()>0)
			colElt.addAttribute("length", String.valueOf(field.getMaxLength()));
		
		if(!keyProp && field.isMandatory())
			colElt.addAttribute("not-null", "true");
	}
	
	private void setAppPropertiesValue(Properties prop,String key,String value )
	{
		String sKey = /*sModuleName +'.'+ */ TableField.mapPropName(tableName) +'.'+key;
		if(! prop.containsKey(sKey))
			prop.setProperty(sKey,  value);
	}
	
	public void addResource(String filename)
	{
		
		try {
			Properties prop = new Properties();
			if( FileSystemOpt.existFile(filename+"_zh_CN.properties")){
			    try(FileInputStream fis  = new FileInputStream(filename+"_zh_CN.properties")){
			    		prop.load(fis);
				}
			}
		    
			setAppPropertiesValue(prop,"list.title",tableLabelName+"列表");
			setAppPropertiesValue(prop,"edit.title","编辑"+tableLabelName);
			setAppPropertiesValue(prop,"view.title","查看"+tableLabelName);
			for(TableField col : columns )
				setAppPropertiesValue(prop,TableField.mapPropName(col.getColumnName()),col.getFieldLabelName());
		    
			try(FileOutputStream outputFile = new FileOutputStream(filename+"_zh_CN.properties")){   
			    prop.store(outputFile, "create by centit B/S framework!");  
			    //outputFile.close(); 
			}
		    //prop.list(System.out);
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			Properties prop = new Properties();
			if( FileSystemOpt.existFile(filename+".properties")){
				try(FileInputStream fis  = new FileInputStream(filename+".properties")){
		    		prop.load(fis);
				}
			}
		    
			setAppPropertiesValue(prop,"list.title",TableField.mapPropName(tableName)+" list");
			setAppPropertiesValue(prop,"edit.title","new or edit "+TableField.mapPropName(tableName)+" piece");
			setAppPropertiesValue(prop,"view.title","view "+TableField.mapPropName(tableName)+" piece");
			for(TableField col : columns )
				setAppPropertiesValue(prop,TableField.mapPropName(col.getColumnName()),col.getPropertyName());
		    
		    try(FileOutputStream outputFile = new FileOutputStream(filename+".properties")){   
			    prop.store(outputFile, "create by centit B/S framework!");   
			    //outputFile.close(); 
		    }
		    //prop.list(System.out);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void saveHibernateMappingFile(String filename ){
		Document doc = null;
	
		if(FileSystemOpt.existFile(filename )){
			System.out.println("文件："+filename+" 已存在！");
			return;
		}

		doc = DocumentHelper.createDocument();
		//doc.addProcessingInstruction("xml", "version=\"1.0\" encoding=\"utf\"");
		doc.addDocType("hibernate-mapping", "-//Hibernate/Hibernate Mapping DTD 3.0//EN", 
				"http://www.hibernate.org/dtd/hibernate-mapping-3.0.dtd");
		doc.addComment("Mapping file autogenerated by codefan@centit.com");
		Element root  = doc.addElement("hibernate-mapping");//首先建立根元素 
		//create class
		Element classElt = root.addElement("class");
		classElt.addAttribute("name", packageName+'.'+getClassName());
		classElt.addAttribute("table", tableName.toUpperCase());
		classElt.addAttribute("schema", schema);
		//save primary key
		if(pkColumns!=null && pkColumns.size()>1){
			Element idElt = classElt.addElement("composite-id");
			idElt.addAttribute("name", "cid");
			idElt.addAttribute("class", packageName+'.'+getClassName()+"Id");
			for(Iterator<String> it = pkColumns.iterator();it.hasNext();){
				String pkcol = it.next();
				TableField field = findField(pkcol);
				if(field!=null){
					Element keyElt = idElt.addElement("key-property");
					saveProperty(field,keyElt,true);
					//colElt.addAttribute("not-null", "true");
				}
			}
		}else if(pkColumns !=null && pkColumns.size()==1){
			Element idElt = classElt.addElement("id");
			TableField field = findField(pkColumns.get(0));
			saveProperty(field,idElt,true);
			Element genElt = idElt.addElement("generator");
			genElt.addAttribute("class", "assigned");
		}
		//save property
		if(columns !=null){
			for(Iterator<TableField> it = columns.iterator();it.hasNext();){
				TableField col = it.next();
				if(isParmaryKey(col.getColumnName()))
					continue;
				Element propElt = classElt.addElement("property");
				saveProperty(col,propElt,false);
			}
		}
		if(references !=null){
			for(Iterator<TableReference> it = references.iterator();it.hasNext();){
				TableReference ref = it.next();
				Element setElt = classElt.addElement("set");
				setElt.addAttribute("name", TableField.mapPropName(ref.getTableName())+'s');
				setElt.addAttribute("cascade", "all-delete-orphan");//"all-delete-orphan")//save-update,delete;
				setElt.addAttribute("inverse", "true");
				Element keyElt = setElt.addElement("key");
				for(Iterator<TableField> it2 = ref.getFkcolumns().iterator();it2.hasNext();){
					TableField col = it2.next();
					Element colElt = keyElt.addElement("column");
					saveColumn(col,colElt,false);
				}
				Element maptypeElt = setElt.addElement("one-to-many");
				maptypeElt.addAttribute("class", packageName+'.'+ ref.getClassName());
			}
		}
		writerXMLFile(doc,filename);		
	}

	public List<TableField> getColumns() {
		if(columns==null)
			columns = new ArrayList<TableField>();
		return columns;
	}

	public void setColumns(List<TableField> columns) {
		this.columns = columns;
	}

	public List<String> getPkColumns() {
		if(pkColumns==null)
			pkColumns = new ArrayList<String>();
		return pkColumns;
	}

	public void setPkColumns(List<String> pkColumns) {
		this.pkColumns = pkColumns;
	}


	public String getClassName() {
		String sClassName = TableField.mapPropName(tableName);
		return sClassName.substring(0,1).toUpperCase() + 
				sClassName.substring(1);	
	}

	public List<TableReference> getReferences() {
		if(references==null)
			references = new ArrayList<TableReference>();
		return references;
	}

	public void setReferences(List<TableReference> references) {
		this.references = references;
	}
}
