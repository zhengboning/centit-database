package com.centit.support.database;

import java.sql.SQLException;

public class DatabaseAccessException extends SQLException {
	
	private static final long serialVersionUID = 1L;

	public DatabaseAccessException(){
		super();
	}
	
	public DatabaseAccessException(String message){
		super(message);
	}
	
	public DatabaseAccessException(SQLException e){
		 super(e.getMessage(), e.getSQLState(), e.getErrorCode(),e.getCause());
		 this.setNextException(e.getNextException());
	}
	
	public DatabaseAccessException(String sql , SQLException e){
		 super(sql +" raise "+ e.getMessage(), e.getSQLState(), e.getErrorCode(),e.getCause());
		 this.setNextException(e.getNextException());
	}
}
