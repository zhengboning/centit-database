package com.centit.support.database;

import java.util.HashSet;
import java.util.Set;

public enum DBType {
	Oracle,DB2,SqlServer,MySql,Access,Unknown;
	
	public static DBType mapDBType(String connurl){
		if(connurl==null)
			return Unknown;
		if("oracle".equalsIgnoreCase(connurl)  
			||	connurl.startsWith("jdbc:oracle"))
			return Oracle;
		if("db2".equalsIgnoreCase(connurl)
			||	connurl.startsWith("jdbc:db2"))
			return DB2;
		if("sqlserver".equalsIgnoreCase(connurl)
			||	connurl.startsWith("jdbc:sqlserver"))
			return SqlServer;
		if("mysql".equalsIgnoreCase(connurl)
			||	connurl.startsWith("jdbc:mysql"))
			return MySql;
		if("access".equalsIgnoreCase(connurl))
			return Access;
		
		return Unknown;
	}
	
	public static Set<DBType> allValues()
	{
		Set<DBType> dbtypes = new HashSet<DBType>();
		dbtypes.add(Oracle);
		dbtypes.add(DB2);
		dbtypes.add(SqlServer);
		dbtypes.add(MySql);
		dbtypes.add(Access);
		
		return dbtypes;
	}
	public static String getDbDriver(DBType dt)
	{
	  switch(dt){
	  	case Oracle:
	  		return "oracle.jdbc.driver.OracleDriver";
	  	case DB2:
	  		return "com.ibm.db2.jdbc.app.DB2Driver";
	  	case SqlServer:
	  		return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	  	case Access:
	  		return "sun.jdbc.odbc.JdbcOdbcDriver";
	  	case MySql:
	  		return "org.gjt.mm.mysql.Driver";
	  	default:
	  		return "";
	  }
	}
}
