package com.capbpm;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.cassandra.core.RowMapper;
import org.springframework.data.cassandra.core.CassandraOperations; 
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.QueryBuilder; 
import com.datastax.driver.core.querybuilder.Select; 

public class AccessDb {
	
	/*
	 * native
	 *    = on primary key | key = val | val comes from user
	 *   in on primary key | key in (val1, val2, ... valX) | val1, val2 ... valX comes from user
	 *   <  on primary key | key in (min, val1) | min is lowest value key can have, val1 comes from user
	 *   >  on primary key | key in (val1, max) | max is greatest value key can have, val1 comes from user
	 *   <=
	 *   =>
	 *   
	 * 
	 */
	
	String[] comparisons = { "=", "in", "<", ">", "<=", ">=", "!=" };
	
	public JSONArray comparisonsAsJson(){
		JSONArray ja = new JSONArray();
		
		for (int idx=0; idx<comparisons.length; idx++){
			ja.put(comparisons[idx]);
		}
		
		return ja;
	}
	public boolean createTable(String createTable){
		boolean result = true;
		Cluster cluster; 
		Session session;
		
		try {
			System.out.println("createTable(); createTable="+createTable);
			cluster = Cluster.builder().addContactPoints(InetAddress.getByName("odm.capbpm.com")).build();
			cluster.init();
			session = cluster.connect("showcase"); 
			
			ResultSet rs = session.execute(createTable);
			
			if (rs == null){
				result = false;
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("AccessCassandra ERROR");
			e.printStackTrace();
		}		
		
		
		return result;
	}
	
    public ResultSet run(String cqlsh){
		boolean result = false;
		Cluster cluster; 
		Session session;
		ResultSet rs = null;
		
		try {
			cluster = Cluster.builder().addContactPoints(InetAddress.getByName("odm.capbpm.com")).build();
			cluster.init();
			session = cluster.connect("showcase"); 
			
			rs = session.execute(cqlsh);
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("AccessCassandra ERROR");
			e.printStackTrace();
		}		
		
		
		return rs;
	}
    
    
 
    public ArrayList<Row> runReturnArrayList(String cqlsh){
		boolean result = false;
		Cluster cluster; 
		Session session;
		ResultSet rs = null;
		ArrayList<Row> resultArrList = new ArrayList<Row>();
		
		try {
			cluster = Cluster.builder().addContactPoints(InetAddress.getByName("odm.capbpm.com")).build();
			cluster.init();
			session = cluster.connect("showcase"); 
			
			rs = session.execute(cqlsh);
			
			Iterator<Row> ir = rs.iterator();
			
			while (ir.hasNext()){
				resultArrList.add(ir.next());
			}
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("AccessCassandra ERROR");
			e.printStackTrace();
		}		
		
		
		return resultArrList;
	}
    
    public ResultSet getTableMetadata(String tableName){
		boolean result = false;
		Cluster cluster; 
		Session session;
		ResultSet rs = null;
		
		try {
			cluster = Cluster.builder().addContactPoints(InetAddress.getByName("odm.capbpm.com")).build();
			cluster.init();
			session = cluster.connect("showcase"); 
			
			rs = session.execute("select column_name, validator, type  from system.schema_columns  where columnfamily_name = '" + tableName + "' allow filtering;");
			
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("AccessCassandra ERROR");
			e.printStackTrace();
		}		
		
		
		return rs;   	
    }

    public JSONObject listColumns(String table) throws JSONException
    {
    	JSONObject joResult = new JSONObject();
    
    	String validator;
    	
		ResultSet columnNameColumnTypeRS = getTableMetadata(table);
	
		Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
	    while (rowIterator.hasNext()){
	    	Row r = rowIterator.next();
	    	validator = r.getString("validator");
	    	
	    	joResult.put(r.getString("column_name"), validator.replace("org.apache.cassandra.db.marshal.", ""));
	    }
    	return joResult;
    }

    /*
     * returns
     
		[
		    {
		        "columnName": "accountnumber",
		        "columnType": "UTF8Type",
		        "isKey": true
		    },
		    {
		        "columnName": "balance",
		        "columnType": "DoubleType",
		        "isKey": false
		    },
		    {
		        "columnName": "isactive",
		        "columnType": "BooleanType",
		        "isKey": false
		    },
		    {
		        "columnName": "startdate",
		        "columnType": "TimestampType",
		        "isKey": false
		    }
		]
     */
    public JSONArray listColumnsAndType(String table) throws JSONException
    {
    	JSONArray jsonArray = new JSONArray();
    	String validator;
    	
		ResultSet columnNameColumnTypeRS = getTableMetadata(table);
	
		Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
	    while (rowIterator.hasNext()){
	    	Row r = rowIterator.next();
	    	validator = r.getString("validator");
	    	
	    	JSONObject jo = new JSONObject();
	    	jo.put("columnName",r.getString("column_name"));
	    	jo.put("columnType",validator.replace("org.apache.cassandra.db.marshal.", ""));
	    	if (r.getString("type").trim().equals("partition_key")){ 
	    	    jo.put("isKey",true);
	    	}
	    	else{
	    		jo.put("isKey", false);
	    	}
	    	jsonArray.put(jo);
	    	//joResult.put(r.getString("column_name"), validator.replace("org.apache.cassandra.db.marshal.", ""));
	    }
    	return jsonArray;
    }
    
    public HashSet<String> listTables(){
    	
    	HashSet<String> hashSet = new HashSet<String>();
    	
    	String listTableString = "select columnfamily_name, column_name from system.schema_columns where keyspace_name = 'showcase'";
	    ResultSet rs = run(listTableString);	
		Iterator<Row> rowIterator = rs.iterator();
	    while (rowIterator.hasNext()){
	    	Row r = rowIterator.next();
            hashSet.add(r.getString("columnfamily_name"));	    	
	    }
	    return hashSet;
    }

}
