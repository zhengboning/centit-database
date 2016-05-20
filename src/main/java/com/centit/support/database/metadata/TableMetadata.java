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

public class TableMetadata {
	
	private List<TableField> columns=null;
	private List<String> pkColumns=null;
	private String sSchema;

	private String sTabName;// 其实是table 代码 code
	//private String sClassName;//表对应的类名 同时作为业务模块名
	private String sTabDesc;// 表的 描述，中文名称
	private String sTabComment;// 表的备注信息 
	private String sPkName;
	private List<ReferenceMetadata> references=null;
	private String sPackageName;
	
	/**
	 * 数据库表名，对应pdm中的code，对应元数据中的 tabcode
	 */
	public String getTabName() {
		return sTabName;
		
	}
	
	public void setTabName(String tabName) {
		sTabName = tabName;

	}	
	
	/**
	 * 数据库表中文名，对应pdm中的name,对应元数据中的 tabname
	 */
	public String getTabDesc() {
		return sTabDesc;
	}

	public void setTabDesc(String tabDesc) {
		this.sTabDesc = tabDesc;
	}

	/**
	 * 数据库表备注信息，对应pdm中的Comment,对应元数据中的 tabdesc
	 */
	public String getTabComment() {
		return sTabComment;
	}

	public void setTabComment(String tabComment) {
		this.sTabComment = tabComment;
	}

	public String getPkName() {
		return sPkName;
	}

	public void setPkName(String pkName) {
		sPkName = pkName;
	}	
	public String getPackageName() {
		return sPackageName;
	}

	public void setPackageName(String packageName) {
		sPackageName = packageName;
	}

	public String getSchema() {
		return sSchema;
	}

	public void setSchema(String schema) {
		sSchema = schema;
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
	
	public TableField findField(String colname){
		for(Iterator<TableField> it = columns.iterator();it.hasNext();){
			TableField col = it.next();
			if(col.getColumn().equals(colname))
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
	
	public TableMetadata()
	{
		
	}
	
	public TableMetadata(String tabname)
	{
		setTabName(tabname);
	}
	
	private void saveProperty(TableField field,Element propElt,boolean keyProp){
		propElt.addAttribute("name", field.getName());
		propElt.addAttribute("type", field.getHibernateType());
		Element colElt = propElt.addElement("column");
		saveColumn(field,colElt,keyProp);
	}
	
	private void saveColumn(TableField field,Element colElt,boolean keyProp){
		colElt.addAttribute("name", field.getColumn().toUpperCase());
		if("Long".equals(field.getType()) || "Double".equals(field.getType()) ){
			colElt.addAttribute("precision", String.valueOf(field.getPrecision()));
			colElt.addAttribute("scale", String.valueOf(field.getScale()));
		}else if(field.getMaxLength()>0)
			colElt.addAttribute("length", String.valueOf(field.getMaxLength()));
		
		if(!keyProp && field.isNotNull())
			colElt.addAttribute("not-null", "true");
	}
	
	private void setAppPropertiesValue(Properties prop,String key,String value )
	{
		String sKey = /*sModuleName +'.'+ */ TableField.mapPropName(sTabName) +'.'+key;
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
		    
			setAppPropertiesValue(prop,"list.title",sTabDesc+"列表");
			setAppPropertiesValue(prop,"edit.title","编辑"+sTabDesc);
			setAppPropertiesValue(prop,"view.title","查看"+sTabDesc);
			for(TableField col : columns )
				setAppPropertiesValue(prop,TableField.mapPropName(col.getColumn()),col.getDesc());
		    
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
		    
			setAppPropertiesValue(prop,"list.title",TableField.mapPropName(sTabName)+" list");
			setAppPropertiesValue(prop,"edit.title","new or edit "+TableField.mapPropName(sTabName)+" piece");
			setAppPropertiesValue(prop,"view.title","view "+TableField.mapPropName(sTabName)+" piece");
			for(TableField col : columns )
				setAppPropertiesValue(prop,TableField.mapPropName(col.getColumn()),col.getName());
		    
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
		classElt.addAttribute("name", sPackageName+'.'+getClassName());
		classElt.addAttribute("table", sTabName.toUpperCase());
		classElt.addAttribute("schema", sSchema);
		//save primary key
		if(pkColumns!=null && pkColumns.size()>1){
			Element idElt = classElt.addElement("composite-id");
			idElt.addAttribute("name", "cid");
			idElt.addAttribute("class", sPackageName+'.'+getClassName()+"Id");
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
				if(isParmaryKey(col.getColumn()))
					continue;
				Element propElt = classElt.addElement("property");
				saveProperty(col,propElt,false);
			}
		}
		if(references !=null){
			for(Iterator<ReferenceMetadata> it = references.iterator();it.hasNext();){
				ReferenceMetadata ref = it.next();
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
				maptypeElt.addAttribute("class", sPackageName+'.'+ ref.getClassName());
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
		String sClassName = TableField.mapPropName(sTabName);
		return sClassName.substring(0,1).toUpperCase() + 
				sClassName.substring(1);	
	}

	public List<ReferenceMetadata> getReferences() {
		if(references==null)
			references = new ArrayList<ReferenceMetadata>();
		return references;
	}

	public void setReferences(List<ReferenceMetadata> references) {
		this.references = references;
	}
}
