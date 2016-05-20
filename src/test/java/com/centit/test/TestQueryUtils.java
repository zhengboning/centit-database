package com.centit.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.centit.support.algorithm.StringBaseOpt;
import com.centit.support.common.KeyValuePair;
import com.centit.support.database.QueryAndNamedParams;
import com.centit.support.database.QueryUtils;

public class TestQueryUtils {
	
	public static void main(String[] args) {
		//System.out.println(QueryUtils.getMatchString("hell%%'%'%wo'rd n__n"));
		testBuildGetCountSQL();//testTemplate();
		//CodeRepositoryUtil.loadExtendedSqlMap("D:/Projects/framework2.1/framework-sys-module2.1/src/main/resources/ExtendedSqlMap.xml");
		//System.out.println(CodeRepositoryUtil.getExtendedSql("QUERY_ID_1"));
	}
	
	public static void testBuildGetCountSQL() {
		System.out.println(QueryUtils.buildGetCountSQL(""));
		System.out.println(QueryUtils.buildGetCountSQL("From UserInfo"));
		System.out.println(QueryUtils.buildGetCountSQL("with(select * from table group by 1,2 order by ab) a "
				+ "select distinct a,b,c,count(1) From (select * from UserInfo group by a, b order by a,b) "
				+ " atable group cute by a.a,b.v,b.c order by 1,2"));
	}
	
	
	public static void printQueryAndNamedParams(QueryAndNamedParams qp) {
		System.out.println(qp.getQuery());
		for(Map.Entry<String, Object>ent : qp.getParams().entrySet()){
			System.out.print(ent.getKey());
			System.out.print("----");
			System.out.println(String.valueOf(ent.getValue()));
		}
	}
	
	public static void printDictionaryMap(Map<String,KeyValuePair<String,String>> m) {

		for(Map.Entry<String, KeyValuePair<String,String>>ent : m.entrySet()){
			System.out.print(ent.getKey());
			System.out.print("----");
			System.out.print(String.valueOf(ent.getValue().getKey()));
			System.out.print("----");
			System.out.println(String.valueOf(ent.getValue().getValue()));
		}
	}
	

	
	public static void testGetParams() {
		
		String queryStatement = "select [(${p1.1}>2 && p2>2)|t1.a,] t2.b,t3.c "+
				"from [(${p1.1}>2  && p2>2)| table1 t1,] table2 t2,table3 t3 "+
				"where 1=1 [(${p1.1}>2  && p2>2)(p1.1:ps)| and t1.a=:ps][(isNotEmpty(${p1.1})&&isNotEmpty(p2)&&isNotEmpty(p3))(p2,p3:px)"
				+ "| and (t2.b> :p2 or t3.c >:px)] order by 1,2";
		System.out.println(QueryUtils.getSqlNamedParameters(queryStatement).getKey());
	}
	
	public static void testTranslateQuery() {
		List<String> filters = new ArrayList<String> ();
		filters.add("[table1.c] like { p1.1: ( datetime ) ps}");
		filters.add("[table1.b] = {( like )p2}");
		filters.add("[table1.c] = {:(like )p2}");
		filters.add("[table4.b] = {p4}");	
		filters.add("([table2.f]={p2} and [table3.f]={p3})");

		Map<String,Object> paramsMap = new HashMap<String,Object>();		
		paramsMap.put("p1.1", "1212年5月6日 下午 5点 25分33秒");
		paramsMap.put("p2", "h w word hello");
		
		/*String queryStatement = "select t1.a,t2.b,t3.c "+
			"from table1 t1,table2 t2,table3 t3 "+
			"where 1=1 {table1:t1} {不认识} [也不认识] order by 1,2";
	
		printQueryAndNamedParams(QueryUtils.translateQuery(
				 queryStatement, filters,
				  paramsMap, true));*/
		
		String queryStatement = "select t1.a,t2.b,t3.c "
					+ "from table1 t1,table2 t2,table3 t3 "
					//+ "where 1=1 {table1:t1}{table9:t1}"
					//+ "{table2:t2,table3:t3}"
					+ "[(count(p1.1,p2)>1)((like )p1.1,p2 : (like )pw)| and tw.a=:p3] "
					//+ " [ p1.1 :()  p4, : ( like )p2  | and tw.a=:p3 ]"
					+ " order by 1,2";
		paramsMap.put("p3", "5");
		paramsMap.put("p4", "7");
		printQueryAndNamedParams(QueryUtils.translateQuery(
				 queryStatement, filters,
				  paramsMap, true));
		/*
		queryStatement = "select [(${p1.1}>2 && p2>2)|t1.a,] t2.b,t3.c "+
				"from [(${p1.1}>2  && p2>2)| table1 t1,] table2 t2,table3 t3 "+
				"where 1=1 [(${p1.1}>2  && p2>2)(p1.1:ps)| and t1.a=:ps][(isNotEmpty(${p1.1})&&isNotEmpty(p2)&&isNotEmpty(p3))(()p2,p3:px)"
				+ "| and (t2.b> :p2 or t3.c >:px)] order by 1,2";

		printQueryAndNamedParams(QueryUtils.translateQuery(
				 queryStatement, filters,
				  paramsMap, true));
		
		paramsMap.put("p1.1", "5");
		queryStatement = "select [(${p1.1}>2 && p2>2)|t1.a,] t2.b,t3.c "+
				"from [(${p1.1}>2 && p2>2)| table1 t1,] table2 t2,table3 t3 "+
				"where 1=1 [(${p1.1}>2 && p2>2)(p1.1:ps)| and t1.a=:ps][p1.1,:p2,p3:px| and (t2.b> :p2 or t3.c >:px)] order by 1,2";
		printQueryAndNamedParams(QueryUtils.translateQuery(
				 queryStatement, filters,
				  paramsMap, true));*/
	}
	
	public static void testTemplate() {
		String queryStatement = "select [(${我是中国人@SINA}>2 && p2>2 )|t1.a,] t2.b,t3.c "+
				"from [(${p1.1}>2 && p2>2)| table1 t1,] table2 t2,table3 t3 "+
				"where t2.usercode = :userName  [(${p1.1}>2 && p2>2)(p5,:p9)| and t1.a=:ps][p3:px| and (t2.b> :p2 or t3.c >:px)] order by 1,2";
		System.out.println(StringBaseOpt.objectToString(
				QueryUtils.getSqlTemplateFiledNames(queryStatement)));
	}
	
}
