package com.centit.support.database;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;

public class DbcpConnectPools {
	
	private static final
		Map<DataSourceDescription,BasicDataSource> dbcpDataSourcePools
		 = new HashMap<DataSourceDescription,BasicDataSource >();
	
	private static BasicDataSource addDataSource(DataSourceDescription dsDesc){
		BasicDataSource ds = new BasicDataSource();  
		ds.setDriverClassName(dsDesc.getDriver());  
		ds.setUsername(dsDesc.getUsername());  
		ds.setPassword(dsDesc.getPassword());  
		ds.setUrl(dsDesc.getConnUrl());  
		ds.setInitialSize(dsDesc.getInitialSize()); // 初始的连接数；  
		ds.setMaxTotal(dsDesc.getMaxTotal());  
		ds.setMaxIdle(dsDesc.getMaxIdle());  
		ds.setMaxWaitMillis(dsDesc.getMaxWaitMillis());  
		ds.setMinIdle(dsDesc.getMinIdle());  
		dbcpDataSourcePools.put(dsDesc, ds);
		return ds;
	}
	
	public static DbcpConnect getDbcpConnect(DataSourceDescription dsDesc){
		BasicDataSource ds = dbcpDataSourcePools.get(dsDesc);
		if(ds==null)
			ds = addDataSource(dsDesc);
		Connection conn = null;
		try {  
			conn = ds.getConnection();  
        } catch (Exception e) {  
            e.printStackTrace(System.err); 
            return null;
        }  

        try {  
        	conn.setAutoCommit(false);  
        } catch (SQLException e) {  
            e.printStackTrace();
            return null;
        }  
        return new DbcpConnect(dsDesc.getUsername(),dsDesc.getDbType(), conn);		
	}
	
	/** 获得数据源连接状态 */  
    public static Map<String, Integer> getDataSourceStats(DataSourceDescription dsDesc){  
        BasicDataSource bds = dbcpDataSourcePools.get(dsDesc);
        if(bds==null)
        	return null;
        Map<String, Integer> map = new HashMap<String, Integer>(2);  
        map.put("active_number", bds.getNumActive());  
        map.put("idle_number", bds.getNumIdle());  
        return map;  
    }  
  
    /** 关闭数据源 */  
    public static void shutdownDataSource(){  
    	for(Map.Entry<DataSourceDescription,BasicDataSource> dbs : dbcpDataSourcePools.entrySet()){
    		try {
				dbs.getValue().close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }  
    
    public static boolean testDataSource(DataSourceDescription dsDesc){
		BasicDataSource ds = new BasicDataSource();  
		ds.setDriverClassName(dsDesc.getDriver());  
		ds.setUsername(dsDesc.getUsername());  
		ds.setPassword(dsDesc.getPassword());  
		ds.setUrl(dsDesc.getConnUrl());  
		ds.setInitialSize(dsDesc.getInitialSize()); // 初始的连接数；  
		ds.setMaxTotal(dsDesc.getMaxTotal());  
		ds.setMaxIdle(dsDesc.getMaxIdle());  
		ds.setMaxWaitMillis(dsDesc.getMaxWaitMillis());  
		ds.setMinIdle(dsDesc.getMinIdle());
		boolean connOk = false;
		try {
			//Class.forName(dsDesc.getDriver()).newInstance();
			//Connection conn = DriverManager.getConnection(dsDesc.getConnUrl(),
					//dsDesc.getUsername(),dsDesc.getPassword());
			Connection conn = ds.getConnection();
			if(conn != null)
				connOk = true;
			conn.close();
			ds.close();
		} catch (Exception e) {
			try {
				ds.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		return connOk;
	}
}
