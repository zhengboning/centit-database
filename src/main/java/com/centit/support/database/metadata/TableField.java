package com.centit.support.database.metadata;

public class TableField {
	private String sName;// 字段属性名称 
	private String sDesc;// 字段的中文名称，PDM中的 DESC 和元数据表格中的Name对应
	private String sType;
	private String sDBType;
	private String sColumn;// 字段代码 PDM中的CODE
	private String sComment;// 字段注释
	private boolean bNotNull;
	private int 	nMaxLength;//最大长度 Only used when sType=String
	private int  nPrecision;//有效数据位数 Only used when sType=Long Number Float
	private int  nScale;//精度 Only used when sType= Long Number Float
	
	public static String mapPropName(String sDBName){
		String sTempName = sDBName.toLowerCase();
		String sTemp2Name = sDBName.toUpperCase();
		int nl = sDBName.length();
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
		sName = mapPropName(sColumn);
	
		if("NUMBER".equalsIgnoreCase(sDBType) ||
		   "INTEGER".equalsIgnoreCase(sDBType)||
		   "DECIMAL".equalsIgnoreCase(sDBType) ){
			if( nScale > 0 )
				sType = "Double";
			else
				sType = "Long";
			if(nMaxLength <= 0)
				nMaxLength = 8;
		}else if("CHAR".equalsIgnoreCase(sDBType) ||
			   "VARCHAR".equalsIgnoreCase(sDBType)||
			   "VARCHAR2".equalsIgnoreCase(sDBType) ){
			sType = "String";
		}else if("DATE".equalsIgnoreCase(sDBType) ||
				   "TIME".equalsIgnoreCase(sDBType)||
				   "DATETIME".equalsIgnoreCase(sDBType) ){
			sType = "Date";
			if(nMaxLength <= 0)
				nMaxLength = 7;
		}else if("TIMESTAMP".equalsIgnoreCase(sDBType) ){
			sType = "Timestamp";
			if(nMaxLength <= 0)
				nMaxLength = 7;
		}else if("CLOB".equalsIgnoreCase(sDBType) /*||
				   "LOB".equalsIgnoreCase(sDBType)||
				   "BLOB".equalsIgnoreCase(sDBType)*/ ){
			sType = "String";
		}else
			sType = sDBType;
	}
	public String getHibernateType(){
		if(sType !=null && ( sType.equals("Date")|| sType.equals("Timestamp")))
			return "java.util."+sType;
		return "java.lang."+sType;
	}
	
	public TableField()
	{
		bNotNull = false;
		nMaxLength = 0;
		nPrecision = 0;//有效数据位数 Only used when sType=Long Number Float
		nScale = 0;//精度 Only used when sType= Long Number Float
	}
	/**
	 * 字段属性名，是通过字段的code转化过来的
	 */
	public String getName() {
		return sName;
	}
	public void setName(String name) {
		sName = name;
	}
	/**
	 * 字段属性java类别
	 */
	public String getType() {
		return sType;
	}
	
	public static String trimType(String st ){
		int p = st.lastIndexOf('.');
		if(p>0)
			return  st.substring(p+1);
		return st;		
	}
	
	public void setType(String st) {
		sType = trimType(st);
	}
	
	/**
	 * 字段中文名，对应Pdm中的name
	 */	
	public String getDesc() {
		return sDesc;
	}
	
	/**
	 * 字段中文名，对应Pdm中的name
	 */	
	public void setDesc(String desc) {
		sDesc = desc;
	}
		
	/**
	 * 字段代码，对应Pdm中的code
	 */	
	public String getColumn() {
		return sColumn;
	}
	
	/**
	 * @param  column 字段代码，对应Pdm中的code
	 */	
	public void setColumn(String column) {
		sColumn = column;
	}

	/**
	 * 字段描述，对应Pdm中的Comment
	 */	
	public String getComment() {
		return sComment;
	}
	
	public void setComment(String comment) {
		sComment = comment;
	}
	
	public boolean isNotNull() {
		return bNotNull;
	}
	
	public void setNotNull(boolean notNull) {
		bNotNull = notNull;
	}
	
	public void setNotNull(String sNotNull) {
		bNotNull = "true".equalsIgnoreCase(sNotNull) ||
			"1".equals(sNotNull);
	}	
	public void setNullEnable(String sNullEnable) {
		bNotNull = 
			("N".equalsIgnoreCase(sNullEnable) ||
					"0".equalsIgnoreCase(sNullEnable));
	}	
	
	/**
	 * 最大长度 Only used when sType=String
	 * 这个和Precision其实可以共用一个字段
	 * @return 最大长度 
	 */	
	public int getMaxLength() {
		return nMaxLength;
	}
	public void setMaxLength(int maxLength) {
		nMaxLength = maxLength;
	}
	
	/**
	 * 有效数据位数 Only used when sType=Long Number Float 
	 * 这个和maxlength其实可以共用一个字段
	 * @return 有效数据位数
	 */
	public int getPrecision() {
		return nPrecision;
	}
	public void setPrecision(int precision) {
		nPrecision = precision;
	}
	/**
	 * 精度 Only used when sType= Long Number Float
	 * @return 精度
	 */
	public int getScale() {
		return nScale;
	}
	public void setScale(int scale) {
		nScale = scale;
	}
	/**
	 * 字段属性在数据库表中的类型
	 */
	public String getDBType() {
		return sDBType;
	}

	public void setDBType(String type) {
		if(type !=null){
			sDBType = type.trim();
			int nPos = sDBType.indexOf('(');
			if(nPos>0)
				sDBType = sDBType.substring(0,nPos);
		}
	}

}
