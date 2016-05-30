package com.centit.support.database.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.centit.support.xml.IgnoreDTDEntityResolver;

/**
 * 这个变量的属性没有和TableInfo一致起来，设计的时候有欠缺
 * @author codefan
 *
 */
public class HibernateMapInfo {
	private String className;
	private String tableName;
	private String tableLabelName;//中文名
	private String tableComment;
	private List<TableField> keyProperties;
	private List<TableField> properties;
	private boolean isMainTable;
	private boolean hasComplexId;
	private String idType;
	private String idName;
	private List<HibernateMapInfo> one2manys;
	private List<TableReference> references;

	public boolean isReferenceColumn(int refPos , String sCol) {
		if(references==null || references.size() ==0 )
			return false;
		int n = references.size();
		if(refPos<0 || n<=0 || refPos >=n)
			return false;
		return references.get(refPos).containColumn(sCol);
	}
	
	public boolean isHasID() {
		return hasComplexId;
	}

	public void setHasID(boolean hasID) {
		this.hasComplexId = hasID;
	}

	public String getIdType() {
		return idType;
	}

	public void setIdType(String idType) {
		this.idType = idType;
	}
	public String getIdName() {
		return idName;
	}

	public void setIdName(String idName) {
		this.idName = idName;
	}
	public boolean isMainTable() {
		return isMainTable;
	}

	public void setClassName(String sClassName) {
		this.className = sClassName;
	}

	public void setTableName(String sTableName) {
		this.tableName = sTableName;
	}

	public String getTableLabelName() {
		return tableLabelName;
	}

	public void setTableLabelName(String sTableDesc) {
		this.tableLabelName = sTableDesc;
	}

	public String getTableComment() {
		return tableComment;
	}

	public void setTableComment(String sTableComment) {
		this.tableComment = sTableComment;
	}
	
	public void setKeyProperties(List<TableField> keyProperties) {
		this.keyProperties = keyProperties;
	}

	public void setProperties(List<TableField> properties) {
		this.properties = properties;
	}

	public void setOne2manys(List<HibernateMapInfo> one2manys) {
		this.one2manys = one2manys;
	}

	public void setReferences(List<TableReference> references) {
		this.references = references;
	}
	
	public void setMainTable(boolean isMT) {
		this.isMainTable = isMT;
	}

	public HibernateMapInfo()
	{
		isMainTable = true;
		hasComplexId = false;
	}
	public static String trimToSimpleClassName(String className)
	{
		String sClassSimpleName = className;
		int p = className.lastIndexOf('.');
		if( p>0)		
			sClassSimpleName = className.substring(p+1);
		return sClassSimpleName;		
	}
	
	public String getClassSimpleName()
	{
		return trimToSimpleClassName(className);
	}
	
	
	private TableField loadField(Element fieldNode)
	{
		TableField field = new TableField();
		field.setPropertyName(fieldNode.attribute("name").getValue());
		
		String sType = "";
		Attribute atType = fieldNode.attribute("type");
		if(atType != null )
			sType = atType.getValue();
		else{
			atType = fieldNode.attribute("class");
			if(atType != null )
				sType = atType.getValue();
		}
			
		field.setJavaType(sType);
		Element columnNode =  fieldNode.element("column");
		if(columnNode != null){
			Attribute attr;
			field.setColumnName(columnNode.attribute("name").getValue());
			attr =  columnNode.attribute("length");
			if(attr != null)
				field.setMaxLength( Integer.valueOf(attr.getValue()));
			attr =  columnNode.attribute("not-null");
			if(attr != null)
				field.setMandatory(attr.getValue());
			attr =  columnNode.attribute("precision");
			if(attr != null)
				field.setPrecision( Integer.valueOf(attr.getValue()));
			attr =  columnNode.attribute("scale");
			if(attr != null)
				field.setScale(Integer.valueOf(attr.getValue()));
		}
		return field;
	}
	
	@SuppressWarnings("unchecked")
	public void loadHibernateMetadata(String sPath ,String sHbmFile)
	{
		keyProperties = new ArrayList<TableField>();
		properties = new ArrayList<TableField>();
		one2manys = null;
		references = null;
		
		try(FileInputStream is  = new FileInputStream(new  File(sPath + sHbmFile))) {
			//InputStream is = getClass().getResourceAsStream(sPath + sHbmFile);
			SAXReader  builder=new SAXReader (false);
			builder.setValidation(false);
			builder.setEntityResolver(new IgnoreDTDEntityResolver());   
			Attribute attr;

			Document doc= builder.read(is);
			Element classNode=doc.getRootElement().element("class");
			className = classNode.attribute("name").getValue();
			attr =  classNode.attribute("table");
			if(attr != null)
				tableName = attr.getValue();
			
			Element idNode = classNode.element("id");
			if(idNode != null){// 单个主键属性
				TableField tf = loadField(idNode);
				hasComplexId = false;
				idType = TableField.trimType(tf.getJavaType());
				idName = tf.getPropertyName();
				keyProperties.add(tf);
			}else{//复合主键值
				idNode = classNode.element("composite-id");//key-property 
				hasComplexId = true;
				idType = idNode.attributeValue("class");
				idName = idNode.attributeValue("name");
				idType = TableField.trimType(idType);
				
				List<Element> keyNodes = (List<Element>) idNode.elements("key-property");
				for(Element key : keyNodes){
					keyProperties.add(loadField(key));
				}
				/*
				 * if(isMainTable){
					keyNodes = (List<Element>) idNode.elements("key-many-to-one");
					for(Element key : keyNodes){
						keyProperties.add(loadField(key));
					}
				}
				
				keyNodes = (List<Element>) idNode.elements("key-one-to-one");
				for(Element key : keyNodes){
					keyProperties.add(loadField(key));
				}
				*/
			}
			List<Element> propNodes = (List<Element>)classNode.elements("property");
			for(Element prop : propNodes){
				properties.add(loadField(prop));					
			}
			if (isMainTable){
				List<Element> setElements = (List<Element>) classNode.elements("set");
				//List<Element> one2manyNodes = (List<Element>)setElement.elements("//one-to-many");
				if(setElements != null && setElements.size()>0){
					one2manys = new ArrayList<HibernateMapInfo>();
					references = new ArrayList<TableReference>();
					for(Element setElement : setElements){
						Element one2manyNode = setElement.element("one-to-many");
						String sSubClassName = one2manyNode.attributeValue("class");
						int p = sSubClassName.lastIndexOf('.');
						if( p > 0 ){
							sSubClassName = sSubClassName.substring(p+1);
						}
			            
						HibernateMapInfo one2many = new HibernateMapInfo();
						one2many.setMainTable(false);
						one2many.loadHibernateMetadata(sPath,sSubClassName+".hbm.xml");
						one2manys.add(one2many);
						
						TableReference ref = new TableReference();
						ref.setReferenceCode(setElement.attributeValue("name"));
						Element keyElt = setElement.element("key");
						List<Element> colElements = (List<Element>) keyElt.elements("column");
						for(Element colElt : colElements){
							TableField field = new TableField();
							field.setColumnName(colElt.attributeValue("name"));
							field.mapToMetadata();
							ref.getFkcolumns().add(field);
						}
 		                // <column name="FKCOL1" precision="22" scale="0" not-null="true" />
						references.add(ref);
					}
				}
			}
		} catch (DocumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	public String getClassName() {
		return className;
	}

	public String getTableName() {
		return tableName;
	}

	public List<TableField> getKeyProperties() {
		if(keyProperties == null)
			keyProperties = new ArrayList<TableField>();
		return keyProperties;
	}

	public List<TableField> getProperties() {
		if(properties == null)
			properties = new ArrayList<TableField>();
		return properties;
	}
	
	public TableField getProperty(int indx)
	{
		int n = getProperties().size();
		if(n < 1 || indx<0 || indx>=n)
			return new TableField();
		return properties.get(indx);
	}
	
	public TableField getKeyProperty(int indx)
	{
		int n = getKeyProperties().size();
		if(n < 1 || indx<0 || indx>=n)
			return new TableField();
		return keyProperties.get(indx);
	}

	public List<HibernateMapInfo> getOne2manys() {
		if(one2manys==null)
			one2manys = new ArrayList<HibernateMapInfo>();
		return one2manys;
	}
	
	public List<TableReference> getReferences(){
		if(references==null)
			references = new ArrayList<TableReference>();
		return references;
	}

}
