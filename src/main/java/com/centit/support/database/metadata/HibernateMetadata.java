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


public class HibernateMetadata {
	private String sClassName;
	private String sTableName;
	private String sTableDesc;
	private String sTableComment;
	private List<TableField> keyProperties;
	private List<TableField> properties;
	private boolean isMainTable;
	private boolean hasID;
	private String sIdType;
	private String sIdName;
	private List<HibernateMetadata> one2manys;
	private List<ReferenceMetadata> references;

	public boolean isReferenceColumn(int refPos , String sCol) {
		if(references==null || references.size() ==0 )
			return false;
		int n = references.size();
		if(refPos<0 || n<=0 || refPos >=n)
			return false;
		return references.get(refPos).containColumn(sCol);
	}
	
	public boolean isHasID() {
		return hasID;
	}

	public void setHasID(boolean hasID) {
		this.hasID = hasID;
	}

	public String getIdType() {
		return sIdType;
	}

	public void setIdType(String idType) {
		sIdType = idType;
	}
	public String getIdName() {
		return sIdName;
	}

	public void setIdName(String idName) {
		sIdName = idName;
	}
	public boolean isMainTable() {
		return isMainTable;
	}

	public void setClassName(String sClassName) {
		this.sClassName = sClassName;
	}

	public void setTableName(String sTableName) {
		this.sTableName = sTableName;
	}

	public String getTableDesc() {
		return sTableDesc;
	}

	public void setTableDesc(String sTableDesc) {
		this.sTableDesc = sTableDesc;
	}

	public String getTableComment() {
		return sTableComment;
	}

	public void setTableComment(String sTableComment) {
		this.sTableComment = sTableComment;
	}
	
	public void setKeyProperties(List<TableField> keyProperties) {
		this.keyProperties = keyProperties;
	}

	public void setProperties(List<TableField> properties) {
		this.properties = properties;
	}

	public void setOne2manys(List<HibernateMetadata> one2manys) {
		this.one2manys = one2manys;
	}

	public void setReferences(List<ReferenceMetadata> references) {
		this.references = references;
	}
	
	public void setMainTable(boolean isMT) {
		this.isMainTable = isMT;
	}

	public HibernateMetadata()
	{
		isMainTable = true;
		hasID = false;
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
		return trimToSimpleClassName(sClassName);
	}
	
	
	private TableField loadField(Element fieldNode)
	{
		TableField field = new TableField();
		field.setName(fieldNode.attribute("name").getValue());
		
		String sType = "";
		Attribute atType = fieldNode.attribute("type");
		if(atType != null )
			sType = atType.getValue();
		else{
			atType = fieldNode.attribute("class");
			if(atType != null )
				sType = atType.getValue();
		}
			
		field.setType(sType);
		Element columnNode =  fieldNode.element("column");
		if(columnNode != null){
			Attribute attr;
			field.setColumn(columnNode.attribute("name").getValue());
			attr =  columnNode.attribute("length");
			if(attr != null)
				field.setMaxLength( Integer.valueOf(attr.getValue()));
			attr =  columnNode.attribute("not-null");
			if(attr != null)
				field.setNotNull(attr.getValue());
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
			sClassName = classNode.attribute("name").getValue();
			attr =  classNode.attribute("table");
			if(attr != null)
				sTableName = attr.getValue();
			
			Element idNode = classNode.element("id");
			if(idNode != null){// 单个主键属性
				TableField tf = loadField(idNode);
				hasID = false;
				sIdType = TableField.trimType(tf.getType());
				sIdName = tf.getName();
				keyProperties.add(tf);
			}else{//复合主键值
				idNode = classNode.element("composite-id");//key-property 
				hasID = true;
				sIdType = idNode.attributeValue("class");
				sIdName = idNode.attributeValue("name");
				sIdType = TableField.trimType(sIdType);
				
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
					one2manys = new ArrayList<HibernateMetadata>();
					references = new ArrayList<ReferenceMetadata>();
					for(Element setElement : setElements){
						Element one2manyNode = setElement.element("one-to-many");
						String sSubClassName = one2manyNode.attributeValue("class");
						int p = sSubClassName.lastIndexOf('.');
						if( p > 0 ){
							sSubClassName = sSubClassName.substring(p+1);
						}
			            
						HibernateMetadata one2many = new HibernateMetadata();
						one2many.setMainTable(false);
						one2many.loadHibernateMetadata(sPath,sSubClassName+".hbm.xml");
						one2manys.add(one2many);
						
						ReferenceMetadata ref = new ReferenceMetadata();
						ref.setReferenceCode(setElement.attributeValue("name"));
						Element keyElt = setElement.element("key");
						List<Element> colElements = (List<Element>) keyElt.elements("column");
						for(Element colElt : colElements){
							TableField field = new TableField();
							field.setColumn(colElt.attributeValue("name"));
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
		return sClassName;
	}

	public String getTableName() {
		return sTableName;
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

	public List<HibernateMetadata> getOne2manys() {
		if(one2manys==null)
			one2manys = new ArrayList<HibernateMetadata>();
		return one2manys;
	}
	
	public List<ReferenceMetadata> getReferences(){
		if(references==null)
			references = new ArrayList<ReferenceMetadata>();
		return references;
	}

}
