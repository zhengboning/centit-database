package com.centit.support.database.metadata;

public class TableField {
	private String propertyName;// 字段属性名称 
	private String fieldLabelName;// 字段的中文名称 label ，PDM中的 Name 和 元数据表格中的Name对应
	private String javaType;// 类型
	private String columnType;// 数据库中的字段类型
	private String columnName;// 字段代码 PDM中的CODE
	private String columnComment;// 字段注释
	private boolean mandatory;
	private int 	maxLength;//最大长度 Only used when sType=String
	private int  precision;//有效数据位数 Only used when sType=Long Number Float
	private int  scale;//精度 Only used when sType= Long Number Float
	
	public static String mapPropName(String dbObjectName){
		String sTempName = dbObjectName.toLowerCase();
		String sTemp2Name = dbObjectName.toUpperCase();
		int nl = dbObjectName.length();
		if(nl<3)
			return sTempName;
		int i=0;
		String sPropName="";
		while(i<nl){
			if(sTempName.charAt(i) != '_' ){
				sPropName = sPropName + sTempName.charAt(i);
				i++;
			}else{
				i++;
				if(i==2)
					sPropName = "";
				else if(i<nl){
					sPropName = sPropName + sTemp2Name.charAt(i);
					i++;
				}				
			}
		}
		return sPropName;
	}
	
	public void mapToMetadata()
	{
		propertyName = mapPropName(columnName);
	
		if("NUMBER".equalsIgnoreCase(columnType) ||
		   "INTEGER".equalsIgnoreCase(columnType)||
		   "DECIMAL".equalsIgnoreCase(columnType) ){
			if( scale > 0 )
				javaType = "Double";
			else
				javaType = "Long";
			if(maxLength <= 0)
				maxLength = 8;
		}else if("CHAR".equalsIgnoreCase(columnType) ||
			   "VARCHAR".equalsIgnoreCase(columnType)||
			   "VARCHAR2".equalsIgnoreCase(columnType) ){
			javaType = "String";
		}else if("DATE".equalsIgnoreCase(columnType) ||
				   "TIME".equalsIgnoreCase(columnType)||
				   "DATETIME".equalsIgnoreCase(columnType) ){
			javaType = "Date";
			if(maxLength <= 0)
				maxLength = 7;
		}else if("TIMESTAMP".equalsIgnoreCase(columnType) ){
			javaType = "Timestamp";
			if(maxLength <= 0)
				maxLength = 7;
		}else if("CLOB".equalsIgnoreCase(columnType) /*||
				   "LOB".equalsIgnoreCase(sDBType)||
				   "BLOB".equalsIgnoreCase(sDBType)*/ ){
			javaType = "String";
		}else
			javaType = columnType;
	}
	
	public String getHibernateType(){
		if(javaType !=null && ( javaType.equals("Date")|| javaType.equals("Timestamp")))
			return "java.util."+javaType;
		return "java.lang."+javaType;
	}
	
	public TableField()
	{
		mandatory = false;
		maxLength = 0;
		precision = 0;//有效数据位数 Only used when sType=Long Number Float
		scale = 0;//精度 Only used when sType= Long Number Float
	}
	/**
	 * 字段属性名，是通过字段的code转化过来的
	 */
	public String getPropertyName() {
		return propertyName;
	}
	public void setPropertyName(String name) {
		propertyName = name;
	}
	/**
	 * 字段属性java类别
	 */
	public String getJavaType() {
		return javaType;
	}
	
	public static String trimType(String st ){
		int p = st.lastIndexOf('.');
		if(p>0)
			return  st.substring(p+1);
		return st;		
	}
	
	public void setJavaType(String st) {
		javaType = trimType(st);
	}
	
	/**
	 * 字段中文名，对应Pdm中的name
	 */	
	public String getFieldLabelName() {
		return fieldLabelName;
	}
	
	/**
	 * 字段中文名，对应Pdm中的name
	 */	
	public void setFieldLabelName(String desc) {
		fieldLabelName = desc;
	}
		
	/**
	 * 字段代码，对应Pdm中的code
	 */	
	public String getColumnName() {
		return columnName;
	}
	
	/**
	 * @param  column 字段代码，对应Pdm中的code
	 */	
	public void setColumnName(String column) {
		columnName = column;
	}

	/**
	 * 字段描述，对应Pdm中的Comment
	 */	
	public String getColumnComment() {
		return columnComment;
	}
	
	public void setColumnComment(String comment) {
		columnComment = comment;
	}
	
	public boolean isMandatory() {
		return mandatory;
	}
	
	public void setMandatory(boolean notnull) {
		this.mandatory = notnull;
	}
	
	public void setMandatory(String notnull) {
		mandatory = 
			("true".equalsIgnoreCase(notnull) ||
					"1".equalsIgnoreCase(notnull));
	}
	
	public void setNullEnable(String nullEnable) {
		mandatory = 
				("N".equalsIgnoreCase(nullEnable) ||
						"0".equalsIgnoreCase(nullEnable));
	}
	
	/**
	 * 最大长度 Only used when sType=String
	 * 这个和Precision其实可以共用一个字段
	 * @return 最大长度 
	 */	
	public int getMaxLength() {
		return maxLength;
	}
	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}
	
	/**
	 * 有效数据位数 Only used when sType=Long Number Float 
	 * 这个和maxlength其实可以共用一个字段
	 * @return 有效数据位数
	 */
	public int getPrecision() {
		return precision;
	}
	public void setPrecision(int precision) {
		this.precision = precision;
	}
	/**
	 * 精度 Only used when sType= Long Number Float
	 * @return 精度
	 */
	public int getScale() {
		return scale;
	}
	public void setScale(int scale) {
		this.scale = scale;
	}
	/**
	 * 字段属性在数据库表中的类型
	 */
	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String type) {
		if(type !=null){
			columnType = type.trim();
			int nPos = columnType.indexOf('(');
			if(nPos>0)
				columnType = columnType.substring(0,nPos);
		}
	}
}
