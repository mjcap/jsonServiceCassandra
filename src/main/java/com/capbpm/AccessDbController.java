package com.capbpm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

//import javax.persistence.Column;
//import javax.persistence.Table;
import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.codec.binary.Base64;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

@RestController
public class AccessDbController {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    private static int upper = 100;
    private static int lower = 1;

    public static String resultStatusKey = "status";

    @RequestMapping(value="/insertLongJson", method = RequestMethod.POST)
    public String insertLongJson(@RequestParam(value="name") String objectName, @RequestParam(value="input") String input){
    	
    	JSONObject joResult = new JSONObject();
    	
    	if ((objectName != null) && (objectName.trim() != "")){
    			//JSONObject inputJsonObj = new JSONObject(input);
    			AccessDb db = new AccessDb();
    			
    			//verify objectName is unique
    			
    			//INSERT INTO jsonObjects (jsonObjectName, jsonObjectValue)
    	    	//  VALUES('obj name', 'obj');
    			//String cqlsh = "insert into jsonObject (objectname, objectvalue) values ('"+objectName+"', '"+inputJsonObj.toString()+"')";
    			String cqlsh = "insert into jsonObject (objectname, objectvalue) values ('"+objectName+"', '"+input+"')";
                System.out.println("insertJson() cqlsh="+cqlsh);
                db.run(cqlsh);
   		
    	}

    	
    	return joResult.toString();
    }
    
    
    /**
     * 
     * @param input 
     *     JSON object to store
     *     
     * @return
     *     JSON object with key=status
     *                      value=id of inserted JSON object on success
     *                            message on failure
     */
    @RequestMapping(value="/insertJson", method = RequestMethod.POST)
    public String insertJson(@RequestParam(value="name") String objectName, @RequestParam(value="input") String input){
    	
    	JSONObject joResult = new JSONObject();
    	
    	if ((objectName != null) && (objectName.trim() != "")){
        	try {
    			JSONObject inputJsonObj = new JSONObject(input);
    			AccessDb db = new AccessDb();
    			
    			//verify objectName is unique
    			
    			//INSERT INTO jsonObjects (jsonObjectName, jsonObjectValue)
    	    	//  VALUES('obj name', 'obj');
    			String cqlsh = "insert into jsonObject (objectname, objectvalue) values ('"+objectName+"', '"+inputJsonObj.toString()+"')";
                System.out.println("insertJson() cqlsh="+cqlsh);
                db.run(cqlsh);
    			
    		} catch (JSONException e) {
    			// TODO Auto-generated catch block
    			System.out.println("insertJson() given invalid JSON input");
    			e.printStackTrace();
    			try {
    				joResult.put("status", "invalid input JSON");
    			} catch (JSONException e1) {
    				// TODO Auto-generated catch block
    				System.out.println("insertJson() unable to create JSON result object");
    			}
    		}    		
    	}

    	
    	return joResult.toString();
    }
    
    @RequestMapping(value="/readJson", method = RequestMethod.POST)
    public String readJson(@RequestParam(value="name") String objectName){
    	
    	JSONObject joResult = new JSONObject();
    	
    	if ((objectName != null) && (objectName.trim() != "")){

    			
    			AccessDb db = new AccessDb();

    			String cqlsh = "select objectvalue from jsonObject where objectname ='"+objectName+"'";
                System.out.println("insertJson() cqlsh="+cqlsh);
                ArrayList<Row> arrayListString = db.runReturnArrayList(cqlsh);
			    Iterator<Row> rowIterator = arrayListString.iterator();
			    while (rowIterator.hasNext()){			    	
			    	Row r = rowIterator.next();
			    	System.out.println("objectvalue="+r.getString("objectvalue"));   
			    	try {
						joResult=new JSONObject(r.getString("objectvalue"));
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						try {
							joResult.put("status", objectName+" associated with invalid JSON");
						} catch (JSONException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					}
			    }
    		    		
    	}
    	System.out.println("readJson() returning: "+joResult.toString());
    	return joResult.toString();
    }   
 
    @RequestMapping(value="/deleteJson", method = RequestMethod.POST)
    public String deleteJson(@RequestParam(value="name") String objectName){
    	
    	JSONObject joResult = new JSONObject();
    	
    	if ((objectName != null) && (objectName.trim() != "")){

    			
    			AccessDb db = new AccessDb();

    			String cqlsh = "delete from jsonObject where objectname ='"+objectName+"'";
                System.out.println("deleteJson() cqlsh="+cqlsh);
                ArrayList<Row> arrayListString = db.runReturnArrayList(cqlsh);
			    Iterator<Row> rowIterator = arrayListString.iterator();
			    while (rowIterator.hasNext()){			    	
			    	Row r = rowIterator.next();
			    	System.out.println("objectvalue="+r.getString("objectvalue"));   
			    	try {
						joResult=new JSONObject(r.getString("objectvalue"));
					} catch (JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						try {
							joResult.put("status", objectName+" associated with invalid JSON");
						} catch (JSONException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
					}
			    }
    		    		
    	}
    	System.out.println("readJson() returning: "+joResult.toString());
    	return joResult.toString();
    } 
    
    @RequestMapping(value="/callWebService", method = RequestMethod.POST)
    public String callWebService(@RequestParam(value="url") String url, @RequestParam(value="username") String username,
    		                     @RequestParam(value="password") String password){
        String result = "";
        String myUrl = url;
        String myUsername = username;
        String myPassword = password;
        
        if ((myUsername != null) && (!myUsername.trim().equals("")) &&  (myPassword != null) && (!myPassword.trim().equals(""))){
   	         RestTemplate restTemplate = new RestTemplate();
   	         String plainCreds = myUsername+":"+myPassword;
   	         byte[] plainCredsBytes = plainCreds.getBytes();
   	         byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
   	         String base64Creds = new String(base64CredsBytes);

   	         HttpHeaders headers = new HttpHeaders();
   	         headers.add("Authorization", "Basic " + base64Creds);

   	         HttpEntity<String> request = new HttpEntity<String>(headers);
   	         ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, request, String.class);
   	         result = response.getBody();        	
        }
        else{
        	 RestTemplate restTemplate = new RestTemplate(); 
         	 result = restTemplate.getForObject(url, String.class);
        }
        return result;
        
    }
    
    @RequestMapping(value="/getSnapshotIdClasses", method = RequestMethod.POST)
    public String getSnapshotIdClasses(@RequestParam(value="input") String input) throws ParserConfigurationException, SAXException, IOException, JSONException{
    	
    	String snapshotId = input;
    	String url="https://bpm.capbpm.com:9443/rest/bpm/wle/v1/assets?snapshotId="+snapshotId;
    	
        System.out.println("getSnapshotIdClasses snapshotId="+snapshotId);
        System.out.println("getSnapshotIdClasses url="+url);
    	/*
    	 * snapshotId = 2064.411527fb-015c-4021-8ab6-6589b4895c92
    	 * 
    	 * https://bpm.capbpm.com:9443/rest/bpm/wle/v1/assets?snapshotId=2064.411527fb-015c-4021-8ab6-6589b4895c92
         */
    	     
    	     RestTemplate restTemplate = new RestTemplate();
    	     String plainCreds = "Danny:passw0rd";
    	     byte[] plainCredsBytes = plainCreds.getBytes();
    	     byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
    	     String base64Creds = new String(base64CredsBytes);

    	     HttpHeaders headers = new HttpHeaders();
    	     headers.add("Authorization", "Basic " + base64Creds);
 
    	     HttpEntity<String> request = new HttpEntity<String>(headers);
    	     ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, request, String.class);
    	     //ResponseEntity<String> response = 
    	     //		 restTemplate.exchange("https%3A%2F%2Fbpm.capbpm.com%3A9443%2Frest%2Fbpm%2Fwle%2Fv1%2Fexposed%2Fprocess", 
    	     //				 HttpMethod.GET, request, String.class);
    	     //ResponseEntity<String> response = 
    	     //		 restTemplate.exchange("https://bpm.capbpm.com:9443/rest/bpm/wle/v1/exposed/process", 
    	     //				 HttpMethod.GET, request, String.class);
    	     String responseStr = response.getBody();
    	     JSONObject jo = new JSONObject(responseStr);
    	     JSONArray ja = jo.getJSONObject("data").getJSONArray("VariableType");
    	     //System.out.println("getSnapshotIdClasses xmlString="+jo.toString(2));
    	     System.out.println("getSnapshotIdClasses xmlString="+ja.toString(2));
    	       
    	     //return ja.toString();
    	     return ja.toString();
    }   
    
    @RequestMapping(value="/getAppNameSnapshotId", method = RequestMethod.POST)
    public String getAppNameSnapshotId(@RequestParam(value="input") String input) throws ParserConfigurationException, SAXException, IOException, JSONException{
    	
        System.out.println("getAppNameSnapshotId input="+input);
    	
    	/*
    	 * <snapshotID>2064.411527fb-015c-4021-8ab6-6589b4895c92</snapshotID>
           <display>Advanced HR Open New Position</display>
    	 */


         	 //RestTemplate restTemplate = new RestTemplate();
         	 //String s = restTemplate.getForObject(input, String.class);
    	     
    	     //RestClient restClient = new RestClient("Danny", "passw0rd");
    	     //String s = restClient.getForObject(input, String.class);
    	     
    	     RestTemplate restTemplate = new RestTemplate();
    	     String plainCreds = "Danny:passw0rd";
    	     byte[] plainCredsBytes = plainCreds.getBytes();
    	     byte[] base64CredsBytes = Base64.encodeBase64(plainCredsBytes);
    	     String base64Creds = new String(base64CredsBytes);

    	     HttpHeaders headers = new HttpHeaders();
    	     headers.add("Authorization", "Basic " + base64Creds);
 
    	     HttpEntity<String> request = new HttpEntity<String>(headers);
    	     ResponseEntity<String> response = restTemplate.exchange(input, HttpMethod.GET, request, String.class);
    	     //ResponseEntity<String> response = 
    	     //		 restTemplate.exchange("https%3A%2F%2Fbpm.capbpm.com%3A9443%2Frest%2Fbpm%2Fwle%2Fv1%2Fexposed%2Fprocess", 
    	     //				 HttpMethod.GET, request, String.class);
    	     //ResponseEntity<String> response = 
    	     //		 restTemplate.exchange("https://bpm.capbpm.com:9443/rest/bpm/wle/v1/exposed/process", 
    	     //				 HttpMethod.GET, request, String.class);
    	     String xmlString = response.getBody();
    	     System.out.println("getAppNameSnapshotId xmlString="+(new JSONObject(xmlString)).toString(2));
    	     //saxParser.parse(input, handler);
    	       
    	     return (new JSONObject(xmlString)).toString();
    }    
    
    @RequestMapping(value="/getComparisons", method = RequestMethod.POST)
    public String getComparisons(){
    	AccessDb adb = new AccessDb();
    	return adb.comparisonsAsJson().toString();
    }

    @RequestMapping(value="/runSavedQuery", method = RequestMethod.POST)
    public String runSavedQuery(@RequestParam(value="input") String input) throws JSONException{
    	
    	String queryName = input;
    	
    	System.out.println("runSavedQuery() queryName="+queryName);
    	String selectStatement;
    	//JSONObject postSelectComps;
    	JSONArray postSelectComps;
    	String cqlsh = "select * from savedquery where queryName = '"+queryName+"'";
    	System.out.println("runSavedQuery() cqlsh="+cqlsh);
    	
    	Iterator<Row> rowIterator;
    	ArrayList<Row> rowsToKeep = new ArrayList<Row>();
    	String validator;
    	HashMap<String,String>columnNameTypeHM = new HashMap<String, String>();
    	JSONArray ja = new JSONArray();
    	String table="";
    	
    	AccessDb adb = new AccessDb();
    	ResultSet rs1 = adb.run(cqlsh);
    	
    	Iterator<Row> rowIterator2 = rs1.iterator();
    	while (rowIterator2.hasNext()){
    		Row r2 = rowIterator2.next();
    		
    		selectStatement = r2.getString("query");
    		if (r2.getString("postselectfilter").equals("{}")){
    		    postSelectComps = null;	
    		}
    		else{
    		    postSelectComps = new JSONArray(r2.getString("postselectfilter"));
    		}
    	
    		System.out.println("runSavedQuery() selectStatement="+selectStatement);
    		System.out.println("runSavedQuery() postSelectComps="+postSelectComps);
    		//select * from tableName
    		//         ^
    		//012345678911111
    		//          01234
    		int indexFrom = selectStatement.indexOf("from");
    		int startIdxTableName = indexFrom + 5;
    		int endIdxTableName = selectStatement.indexOf(" ", startIdxTableName);
    		if (endIdxTableName == -1){
    			table = selectStatement.substring(startIdxTableName);
    		}
    		else{
    			table = selectStatement.substring(startIdxTableName, endIdxTableName);
    		}
    		
    		System.out.println("runSavedQuery() table="+table);
    		
			ResultSet columnNameColumnTypeRS = adb.getTableMetadata(table);
			
			Iterator<Row> rowIterator3 = columnNameColumnTypeRS.iterator();
		    while (rowIterator3.hasNext()){
		    	Row r = rowIterator3.next();
		    	System.out.println("runSavedQuery() columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
		    	validator = r.getString("validator");
		    	
			    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "double");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "int");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "text");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
			    }
		    
		    	
		    }
    		

    		
		    /*ResultSet rs = adb.run(selectStatement);

		    rowIterator = rs.iterator();
		    while (rowIterator.hasNext()){
		    	
		    	Row r = rowIterator.next();
		    	if (postSelectComps == null){
		    		rowsToKeep.add(r);
		    	}
		    	else{
	                if (secondaryFilter(r, postSelectComps, columnNameTypeHM )){
	                	rowsToKeep.add(r);
	                }
		    	}
		    }*/
			ArrayList<Row> rs = adb.runReturnArrayList(selectStatement);

			if ((postSelectComps != null) && (postSelectComps.length() > 0)){
				for (int idx=0; idx<postSelectComps.length(); idx++ ){
					
					rowsToKeep = new ArrayList<Row>();
					
				    rowIterator = rs.iterator();
				    while (rowIterator.hasNext()){			    	
				    	Row r = rowIterator.next();
				    	if (postSelectComps.getJSONObject(idx) == null){
				    		rowsToKeep.add(r);
				    	}
				    	else{
			                if (secondaryFilter(r, postSelectComps.getJSONObject(idx), columnNameTypeHM )){
			                	rowsToKeep.add(r);
			                }
				    	}	                
				    }
				    rs = rowsToKeep;
				}				
			}
			else{
				rowsToKeep = rs;
				System.out.println("runSavedQuery() postSelectComps is null or 0 length"); 
			}
		    
		    Iterator<Row> arrayListIt = rowsToKeep.iterator();
		    while (arrayListIt.hasNext()){
		    	Row r = arrayListIt.next();
		    	String rowString = "{";
		    	Iterator<String> columnNameTypeKeyIT = columnNameTypeHM.keySet().iterator();

		    	while (columnNameTypeKeyIT.hasNext()){

		    		String column = columnNameTypeKeyIT.next();
		    		String type = columnNameTypeHM.get(column);
		    	    	
					if (type.compareTo("timestamp") == 0){
						  Date result = r.getDate(column);
                          rowString = rowString + " \"" + column+"\":\"" + r.getDate(column) + "\"";
					}
					else if (type.compareTo("double") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getDouble(column) + "\"";
					}else if (type.compareTo("int") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getInt(column) + "\"";
					}else if (type.compareTo("text") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getString(column) + "\"";
					}
					else if (type.compareTo("boolean") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getBool(column) + "\"";
					}						

					if (columnNameTypeKeyIT.hasNext()){
						rowString = rowString + ",";
					}
		    	}		    	
		    	rowString = rowString + "}";
		    	System.out.println("row="+rowString);
		    	JSONObject joResult = new JSONObject(rowString);
		    	System.out.println("joResult="+joResult);
		    	ja.put(joResult);		    	
		    }
    		
    		
    	}
    	return ja.toString();
    }
 

    @RequestMapping(value="/saveQuery", method = RequestMethod.POST)
    public String saveQuery(@RequestParam(value="input") String input) throws JSONException, ParseException{
    	
    	String queryName="", query="";
    	JSONObject jsonReturnObject=new JSONObject();
    	
    	JSONArray ja = new JSONArray();
    	String comparisonClause = "";
    	//JSONArray colType = null;
    	String validator;
    	//JSONObject postSelectComps;
    	JSONArray postSelectComps;
    	boolean include;
        ArrayList<Row> rowsToKeep = new ArrayList<Row>();    	
    	System.out.println("saveQuery() input="+input);
    	JSONObject inputJO = new JSONObject(input);
    	queryName=inputJO.getString("queryName");
    	query=inputJO.getString("query");
    	
    	System.out.println("saveQuery() queryName="+queryName);
    	System.out.println("saveQuery() query="+query);
    	
    	AccessDb ac = new AccessDb();
    	
    	
    	HashMap<String,String>columnNameTypeHM = new HashMap<String, String>();
    	
    	//{"table":"customer", "comps":[ {"column":"accountbalance" , "comp":"<", "type":"double", "value":"4.00" }]}
    	

			JSONObject jo = new JSONObject(query);
			String table = jo.getString("table").toLowerCase();
			comparisonClause = this.generateComps(jo, table);
			postSelectComps = this.generatePostSelectComps(jo, table);
			if (postSelectComps == null){
			   postSelectComps = new JSONArray();
			}

			System.out.println("read comparisonClause="+comparisonClause);
			
			ResultSet columnNameColumnTypeRS = ac.getTableMetadata(table);
			
			
			//DETERMINE IF THIS IS NECESSARY FOR RAM
			System.out.println("crudService GetController.read()");
			Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
		    while (rowIterator.hasNext()){
		    	Row r = rowIterator.next();
		    	System.out.println("columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
		    	validator = r.getString("validator");
		    	
			    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "double");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "int");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "text");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
			    }
		    
		    	
		    }
			
		    String selectStatement;
		    if (comparisonClause.equals("")){
				  selectStatement = "select * from " + table;		    	
		    }
		    else{
		      comparisonClause = comparisonClause.replaceAll("'","''");
			  selectStatement = "select * from " + table + " where " + comparisonClause;
		    }
		    
		    System.out.println("saveQuery() queryName="+queryName);
			System.out.println("saveQuery() selectStmt="+selectStatement);
			System.out.println("saveQuery() postSelectComps="+postSelectComps);
    	
    	
    	
    	
    	AccessDb adb = new AccessDb();
    	
    	//INSERT INTO users (user_id, first_name, last_name, emails)
    	//  VALUES('frodo', 'Frodo', 'Baggins', {'f@baggins.com', 'baggins@gmail.com'});
    	    	
    	ResultSet rs = adb.run("insert into savedquery (queryname, query, postselectfilter) " +
		    	        "values('"+queryName+"', '"+selectStatement+"', "+"'"+postSelectComps.toString()+"')");

		if (rs != null){
			jsonReturnObject.put("status", "success");
		}
		else{
			jsonReturnObject.put("status", "fail");
		}    	

    	return jsonReturnObject.toString();
    }

    @RequestMapping(value="/mapDbToXsd", method = RequestMethod.POST)
    public String mapDbToXsd(@RequestParam(value="input") String input) throws JSONException
    {
    	  JSONObject jsonReturnObject = new JSONObject();
    	  JSONObject jo = new JSONObject(input);
          System.out.println("mapDbToXsd() input="+input);
          System.out.println("mapDbToXsd() jo="+jo.toString(2));
          String tableName = jo.getString("tableName");
          String mapping = jo.getJSONArray("mapping").toString();
          
          AccessDb ac = new AccessDb();
          ResultSet rs = ac.run("insert into objectmapping (tablename, mapping) values ('"+
   		       tableName +"','" + mapping+"')");
  		  if (rs != null){
			jsonReturnObject.put("status", "success");
		  }
		  else{
			jsonReturnObject.put("status", "fail");
		  } 
    	  return jsonReturnObject.toString();
    	  
    }
    
    @RequestMapping(value="/mapXsdToDb", method = RequestMethod.POST)
    public String mapXsdToDb(@RequestParam(value="input") String input) throws JSONException
    {
    
    	String tableName = null;
    	HashMap<String, ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();
    	ArrayList<Column> columnArrayList = new ArrayList<Column>();
    	HashMap<String, JSONArray> mappingHash = new HashMap<String, JSONArray>();
    	JSONArray mapping = new JSONArray();
    	
    	System.out.println("mapXsdToDb input="+input);
        JSONObject jsonObject = new JSONObject(input);
        System.out.println("mapXsdToDb jsonObject="+jsonObject.toString(2));
        JSONArray jsonArray = jsonObject.getJSONArray("mapping");
        System.out.println("jsonArray="+jsonArray.toString((2)));
        for (int idx=0; idx < jsonArray.length(); idx++){
        	JSONArray subArray = jsonArray.getJSONArray(idx);
        	
        	//{"xsdName":"Account","columnName":"accountNumber","columnType":"xs:string","isKey":true}
            JSONObject jo1 = subArray.getJSONObject(0);
            
            //{"tableName":"account","columnName":"accountnumber","columnType":"UTF8Type"}
            JSONObject jo2 = subArray.getJSONObject(1);
            if (tableName == null){
            	tableName = jo2.getString("tableName");
            	mapping = new JSONArray();
            }
            else{
	            if (!tableName.equals(jo2.getString("tableName"))){
	            	tableNameAndColumns.put(tableName, columnArrayList);
	            	mappingHash.put(tableName, mapping);
	            	columnArrayList = new ArrayList<Column>();
	            	tableName = jo2.getString("tableName");
	            	mapping = new JSONArray();
	            }
            }
            Column c = new Column(jo2.getString("columnName").toLowerCase(), jo2.getString("columnType"));
            if (jo1.getBoolean("isKey")){
            	c.setKey(true);
            }
            columnArrayList.add(c);
            System.out.println(jo1+" will map to "+jo2);
            mapping.put(jo1+"|"+jo2);
        }
        tableNameAndColumns.put(tableName, columnArrayList);
    	mappingHash.put(tableName, mapping);
    	
    	AccessDb ac = new AccessDb();
    	
    	Iterator i = mappingHash.keySet().iterator();
    	while (i.hasNext()){
    		String mappingKey = (String)i.next();
    		System.out.println("mappingKey="+mappingKey+" value:");
    		System.out.println(mappingHash.get(mappingKey).toString(2));
        	// INSERT INTO Hollywood.NerdMovies (user_uuid, fan)
        	//  VALUES (cfd66ccc-d857-4e90-b1e5-df98a3d40cd6, 'johndoe')
        	
    		ac.run("insert into mapping (tableName, xsdtodbmappings) values ('"+
    		       mappingKey +"','" + mappingHash.get(mappingKey).toString()+"')");
    	}
        ArrayList<String> createStrings = generateCreateStrings2(tableNameAndColumns);
       
        JSONObject joResult = new JSONObject();
        
        for (String cString:createStrings){
 		   System.out.println("cString="+cString);
  	       
  	       if (ac.createTable(cString)){
  	          joResult = new JSONObject();
 			  joResult.put("status", "success");
  	       }
  	       else{
   	          joResult = new JSONObject();
  			  joResult.put("status", "error"); 	    	   
  	       }
        }
		

    	return joResult.toString();
    }    

    @RequestMapping(value="/generateTableTemplate", method = RequestMethod.POST)
    public String generateTableTemplate(@RequestParam(value="input") String input) throws JAXBException, JSONException{
         String result = "";
         String mainTableName = null;
         String tableName = null;
         boolean mapFlag = false, mapEntryFlag = false;
         boolean done = false;
         JSONObject jsonResult = new JSONObject();
         jsonResult.put("status", "error");
         
         ArrayList<String> tableNameArrList = new ArrayList<String>();      
         HashMap<String,ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();

     	 RestTemplate restTemplate = new RestTemplate(); 
     	 String s = restTemplate.getForObject(input, String.class);
 	     String[] xsdArray = s.split("\\r?\\n");
 	     Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();    
 	     for (String line : xsdArray){
 	    	line = line.trim();
 	    	line = line.replace("<", "");
 	    	line = line.replace(">", "");
 	    	
 	    	System.out.println("line=["+line+"]");
 	    	String[] lineStrArr = line.split(" ");
 	    	for (String element : lineStrArr){
 	    		//this is the START of the xs:complexType tag and indicates we have a table
 	    		if (element.equals("xs:complexType")){
            		String complexTypeName=findValue(lineStrArr,"name=");
            		if (complexTypeName.equals("Map")){
            			mapFlag = true;
            			//done = false;
            			//mapFlag = false;
            			mapEntryFlag = false;
            			//tableName = mainTableName+"Map";
            			//tableNameArrList.add(tableName);
        			    //tableNameAndColumns.put(tableName, new ArrayList<Column>());
        			    
            			mapEntryFlag = false;
            		}
            		else if (complexTypeName.equals("MapEntry")){
            			        done = false;
            			        mapFlag = false;
            			        mapEntryFlag = true;
	            				tableName = mainTableName+"MapEntry";
	            			    tableNameArrList.add(tableName);
	            			    tableNameAndColumns.put(tableName, new ArrayList<Column>());
	            	}
	            	else{    	
	            		        done = false;
	            		        mapFlag = false;
	            		        mapEntryFlag = false;
	            		        
	                    		mainTableName = complexTypeName;
	                    		tableName = complexTypeName;
	                    		tableNameArrList.add(tableName);
	                    		tableNameAndColumns.put(tableName, new ArrayList<Column>());  
	            	}	            	
            	}
            	else if ((element.equals("xs:element")) && (mapFlag == false) && (done == false)){            		
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		System.out.println("keyName="+keyName+" valueType="+valueType);
            		if (valueType.equals("tns:Map")){
            		  keyName = keyName+tableName+"MapEntry";
            		  valueType = "xs:string";	
            		}
            		else if (valueType.equals("xs:anyType")){
            		  valueType = "xs:string";
            		}
            		else if (valueType.equals("tns:MapEntry")){
            			valueType = "xs:string";
            		}
            		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
            		keyNameValueTypePairs.put(keyName, valueType);
            		
            		Column col = new Column(keyName, valueType);
            		ArrayList columnArrList = tableNameAndColumns.get(tableName);
            		if (columnArrList.size() == 0){
            			col.setKey(true);
            		}
            		columnArrList.add(col);
            		tableNameAndColumns.put(tableName,columnArrList);
            	} 	
 	    		//this is the END of the xs:complexType tag and indicates the end of this table's definition
            	else if (element.equals("/xs:complexType")){
            		done = true;
            	}
 	    	} 	    	
 	     }
	     
 	     jsonResult = new JSONObject();
         Iterator i = tableNameAndColumns.keySet().iterator();
         while (i.hasNext()){
        	 String tabName = (String) i.next();
        	 ArrayList<Column> columnArrList = tableNameAndColumns.get(tabName);
        	 
        	 JSONArray ja = new JSONArray();
        	 for(Column c:columnArrList){
        		 System.out.println("adding "+c.toJSONCassandraObjectString());
        		 ja.put(new JSONObject(c.toJSONCassandraObjectString()));
        	 }
        	 jsonResult.put(tabName, ja);
         }
         
 	     System.out.println("generateTableTemplate jsonResult="+jsonResult.toString(2));
         return jsonResult.toString();
    }    
    
    @RequestMapping(value="/generateXsdTemplate", method = RequestMethod.POST)
    public String generateXsdTemplate(@RequestParam(value="input") String input) throws JAXBException, JSONException{
         String result = "";
         String mainTableName = null;
         String tableName = input;
         boolean mapFlag = false, mapEntryFlag = false;
         boolean done = false;
         JSONObject jsonResult = new JSONObject();
         
         //get column types  
     	AccessDb ad = new AccessDb();
 		JSONObject joTableColumnAndTypes = ad.listColumns(tableName);
		Iterator i = joTableColumnAndTypes.keys();
		
		while (i.hasNext()){
			String columnName = (String)i.next();
			String columnType = joTableColumnAndTypes.getString(columnName);
			
		    System.out.println("generateXsdTemplate columnName="+columnName+" columnType="+columnType);
		    if (columnType.equals("UTF8Type")){
		    	jsonResult.put(columnName, "xs:string");
		    }
		    else if (columnType.equals("DoubleType")){
		    	jsonResult.put(columnName, "xs:double");
		    }
		    else if (columnType.equals("BooleanType")){
		    	jsonResult.put(columnName, "xs:boolean");
		    }
		    else if (columnType.equals("TimestampType")){
                jsonResult.put(columnName,  "xs:dateTime");
		    }
		    else if (columnType.equals("Int32Type")){
		    	jsonResult.put(columnName,  "xs:int");
		    }
		    
		}
         //return JSON of name:type mappings
                  
         return jsonResult.toString();
    }     
    
    @RequestMapping(value="/generateTable3", method = RequestMethod.POST)
    public String generateTables(@RequestParam(value="input") String input) throws JAXBException, JSONException{
         String result = "";
         String mainTableName = null;
         String tableName = null;
         boolean mapFlag = false, mapEntryFlag = false;
         boolean done = false;
         JSONObject jsonResult = new JSONObject();
         jsonResult.put("status", "error");
         
         System.out.println("generateTables() generateTable3 input="+input);
         ArrayList<String> tableNameArrList = new ArrayList<String>();      
         HashMap<String,ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();

     	 RestTemplate restTemplate = new RestTemplate(); 
     	 String s = restTemplate.getForObject(input, String.class);
 	     String[] xsdArray = s.split("\\r?\\n");
 	     //Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();    
 	     for (String line : xsdArray){
 	    	line = line.trim();
 	    	line = line.replace("<", "");
 	    	line = line.replace(">", "");
 	    	
 	    	System.out.println("line=["+line+"]");
 	    	String[] lineStrArr = line.split(" ");
 	    	for (String element : lineStrArr){
 	    		//this is the START of the xs:complexType tag and indicates we have a table
 	    		if (element.equals("xs:complexType")){
            		String complexTypeName=findValue(lineStrArr,"name=");
            		if (complexTypeName.equals("Map")){
            			mapFlag = true;
            			//done = false;
            			//mapFlag = false;
            			mapEntryFlag = false;
            			//tableName = mainTableName+"Map";
            			//tableNameArrList.add(tableName);
        			    //tableNameAndColumns.put(tableName, new ArrayList<Column>());
        			    
            			mapEntryFlag = false;
            		}
            		//else if (complexTypeName.equals("MapEntry")){
            		else if (!complexTypeName.equals(mainTableName)){
            			        done = false;
            			        mapFlag = false;
            			        mapEntryFlag = true;
	            				//tableName = mainTableName+"mapentry";
            			        tableName = complexTypeName;
	            			    tableNameArrList.add(tableName);
	            			    tableNameAndColumns.put(tableName, new ArrayList<Column>());
	                    		Column col = new Column("id", "xs:int");
                    			col.setKey(true);
                    			
	                    		ArrayList columnArrList = tableNameAndColumns.get(tableName);
	                    		columnArrList.add(col);
	                    		tableNameAndColumns.put(tableName,columnArrList);
	            	}
	            	else{    	
	            		        done = false;
	            		        mapFlag = false;
	            		        mapEntryFlag = false;
	            		        
	                    		mainTableName = complexTypeName.toLowerCase();
	                    		tableName = complexTypeName.toLowerCase();
	                    		tableNameArrList.add(tableName);
	                    		tableNameAndColumns.put(tableName, new ArrayList<Column>());  
	            	}	            	
            	}
            	else if ((element.equals("xs:element")) && (mapFlag == false) && (done == false)){            		
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		System.out.println("keyName="+keyName+" valueType="+valueType);
            		if (valueType.equals("tns:Map")){
            		  keyName = keyName+tableName+"MapEntry";
            		  valueType = "xs:string";	
            		}
            		else if (valueType.equals("xs:anyType")){
            		  valueType = "xs:string";
            		}
            		else if (valueType.equals("tns:MapEntry")){
            			valueType = "xs:string";
            		}
            		else if (valueType.startsWith("tns:")){
            			valueType = "xs:int";
            		}
            		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
            		//keyNameValueTypePairs.put(keyName, valueType);
            		
            		Column col = new Column(keyName, valueType);
            		ArrayList columnArrList = tableNameAndColumns.get(tableName);
            		if (columnArrList.size() == 0){
            			col.setKey(true);
            		}
            		columnArrList.add(col);
            		tableNameAndColumns.put(tableName,columnArrList);
            	} 	
 	    		//this is the END of the xs:complexType tag and indicates the end of this table's definition
            	else if (element.equals("/xs:complexType")){
            		done = true;
            	}
 	    	} 	    	
 	     }
	     

 		 ArrayList<String> createStringArrayList = generateCreateStrings(tableNameAndColumns);
 		 for (String cString:createStringArrayList){
			System.out.println("generateTables() generateTable3 cString="+cString);
 		 }
 		
 		 System.out.println("create strings:");
		 for (String cString:createStringArrayList){
			System.out.println(cString);
 	       AccessDb ac = new AccessDb();
 	       if (ac.createTable(cString)){
 	          jsonResult = new JSONObject();
			  jsonResult.put("status", "success");
 	       }
 	       else{
  	          jsonResult = new JSONObject();
 			  jsonResult.put("status", "error"); 	    	   
 	       }
		 } 
         return jsonResult.toString();
    }
 
    public ArrayList<String> generateCreateStrings2(HashMap<String, ArrayList<Column>> tableNameAndColumns){
    	ArrayList<String> result = new ArrayList();
    	String createString;
    	String tableName;
    	
    	Iterator<String> i = tableNameAndColumns.keySet().iterator();
    	while (i.hasNext()){
    	   tableName = i.next();
    	   createString = "CREATE TABLE "+tableName + "(";
    	   System.out.println(createString);
    	   ArrayList<Column> columns = tableNameAndColumns.get(tableName);
    	   String primaryKey = null;
    	   
    	   for (Column col: columns){
    		   String colName = col.getColumnName();
    		   String colType = col.getColumnType();
    		   String type = null;
    		   boolean isPrimaryKey = col.isKey();
    		   
    		   if (!createString.equals("CREATE TABLE "+tableName + "(")){
    			   createString = createString + ", ";
    	    	   System.out.println(createString);
    		   }
    		   
    		   type = colType;
    		      	    	   
   	    	   createString = createString + colName + " " + type;
   	    	   System.out.println(createString);
   	    	   
   	    	   if (col.isKey()){
   	    		   primaryKey = col.getColumnName();
   	    	   }
    	   }
    	   
    	   if (primaryKey != null){
    	       createString = createString  + ", PRIMARY KEY (" + primaryKey +"));";
        	   System.out.println(createString);
    	   }
    	   else{
    		   createString = createString + ")";
        	   System.out.println(createString);
    	   }
    	   
    	   result.add(createString);
    	}
    			 
    	for (String s:result){
    		System.out.println("generateCreateStrings2 result contains s="+s);
    	}
    	return result;
    }    
    
    public ArrayList<String> generateCreateStrings(HashMap<String, ArrayList<Column>> tableNameAndColumns){
    	ArrayList<String> result = new ArrayList();
    	String createString;
    	String tableName;
    	
    	Iterator<String> i = tableNameAndColumns.keySet().iterator();
    	while (i.hasNext()){
    	   tableName = i.next();
    	   createString = "CREATE TABLE "+tableName + "(";
    	   ArrayList<Column> columns = tableNameAndColumns.get(tableName);
    	   String primaryKey = null;
    	   
    	   for (Column col: columns){
    		   String colName = col.getColumnName();
    		   String colType = col.getColumnType();
    		   String type = null;
    		   boolean isPrimaryKey = col.isKey();
    		   
    		   if (!createString.equals("CREATE TABLE "+tableName + "(")){
    			   createString = createString + ", ";
    		   }
   	    	   if (colType.equals("xs:string")){
	    		type = "text";
	    	   }
	    	   else if (colType.equals("xs:dateTime")){
	    		type = "timestamp";
	    	   }
	    	   else if (colType.equals("xs:boolean")){
	    		type = "boolean";
	    	   }
	    	   else if (colType.equals("xs:int")){
	    		type = "int";
	    	   }
	    	   else if (colType.equals("xs:double")){
	    		type = "double";
	    	   }    		   
   	    	   
   	    	   createString = createString + colName + " " + type;
   	    	   
   	    	   if (col.isKey()){
   	    		   primaryKey = col.getColumnName();
   	    	   }
    	   }
    	   
    	   if (primaryKey != null){
    	       createString = createString  + ", PRIMARY KEY (" + primaryKey +"));";
    	   }
    	   else{
    		   createString = createString + ")";
    	   }
    	   
    	   result.add(createString);
    	}
    			 
    	return result;
    }
    
    @RequestMapping(value="/deleteTable",method = RequestMethod.POST)
    public String deleteTable(@RequestParam(value="input") String input) throws JSONException{
    	JSONObject jsonResult = null;
    	
    	AccessDb ac = new AccessDb();
    	
    	String tableName = input.toLowerCase();
        ResultSet rsForColumnName = ac.getTableMetadata(tableName);
		Iterator<Row> rowIterator = rsForColumnName.iterator();
	    while (rowIterator.hasNext()){
	    	Row r = rowIterator.next();
	    	String columnName = r.getString("column_name");
	    	
	    	if (columnName.indexOf("mapentry") != -1){
	    		String secondaryTableToDelete = columnName.replace("alias", "");
	    		ResultSet rs = ac.run("drop table "+secondaryTableToDelete);
	    		if (rs == null){
	  	          jsonResult = new JSONObject();
				  jsonResult.put("status", "error");	    			
	    		}
	    	}	    	
	    }        
        
        if (jsonResult == null){
        	String deleteTableString = "drop table "+tableName;
 	       
 	       ResultSet rs = ac.run(deleteTableString);
 	       System.out.println("rs=["+rs);
 	       if (rs != null){
 	          jsonResult = new JSONObject();
 			  jsonResult.put("status", "success");
 	       }
 	       else{
 	          jsonResult = new JSONObject();
 			  jsonResult.put("status", "error"); 	    	   
 	       }        	
        }
    	
    	return jsonResult.toString();
    }
    
    @RequestMapping(value="/listTables",method = RequestMethod.POST)
    public String listTables() throws JSONException{
    	JSONObject jsonResult = new JSONObject();
    	JSONArray jsonArray = new JSONArray();
    	
    	//jsonResult.put("status", "error");
    	
    	String listTableString = "select columnfamily_name, column_name from system.schema_columns where keyspace_name = 'showcase'";
	    AccessDb ac = new AccessDb();
	    HashSet<String> tableNames = ac.listTables();

	    Iterator tableNamesIterator = tableNames.iterator();
	    
	    while (tableNamesIterator.hasNext()){
	    	String tableName = (String)tableNamesIterator.next();
	    	jsonArray.put(tableName);
	    }
	    
	    jsonResult.put("tableNames", jsonArray);
    	return jsonResult.toString();
    }    
    
    @RequestMapping(value="/listColumns", method = RequestMethod.POST)
    public String listColumns(@RequestParam(value="input") String input) throws JSONException
    {
    
    	String table = input;
    	String validator;
    	
    	AccessDb ad = new AccessDb();
		JSONArray jaResult = ad.listColumnsAndType(table);

    	return jaResult.toString();
    }
    
    @RequestMapping(value="/listXsd", method = RequestMethod.POST)
    public String listXsd(@RequestParam(value="input") String input) throws JSONException, JsonProcessingException
    {
    	
        String result = "";
        String mainTableName = null;
        String tableName = null;
        boolean mapFlag = false, mapEntryFlag = false;
        boolean done = false;
        JSONObject jsonResult = new JSONObject();
        jsonResult.put("status", "error");
        
        ArrayList<String> tableNameArrList = new ArrayList<String>();      
        HashMap<String,ArrayList<Column>> tableNameAndColumns = new HashMap<String, ArrayList<Column>>();

    	 RestTemplate restTemplate = new RestTemplate(); 
    	 String s = restTemplate.getForObject(input, String.class);
	     String[] xsdArray = s.split("\\r?\\n");
	     Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();    
	     for (String line : xsdArray){
	    	line = line.trim();
	    	line = line.replace("<", "");
	    	line = line.replace(">", "");
	    	
	    	System.out.println("line=["+line+"]");
	    	String[] lineStrArr = line.split(" ");
	    	for (String element : lineStrArr){
	    		//this is the START of the xs:complexType tag and indicates we have a table
	    		if (element.equals("xs:complexType")){
           		String complexTypeName=findValue(lineStrArr,"name=");
           		if (complexTypeName.equals("Map")){
           			mapFlag = true;
           			mapEntryFlag = false;
           		}
           		else if (complexTypeName.equals("MapEntry")){
           			        done = false;
           			        mapFlag = false;
           			        mapEntryFlag = true;
	            				tableName = mainTableName+"MapEntry";
	            			    tableNameArrList.add(tableName);
	            			    tableNameAndColumns.put(tableName, new ArrayList<Column>());
	            	}
	            	else{    	
	            		        done = false;
	            		        mapFlag = false;
	            		        mapEntryFlag = false;
	            		        
	                    		mainTableName = complexTypeName;
	                    		tableName = complexTypeName;
	                    		tableNameArrList.add(tableName);
	                    		tableNameAndColumns.put(tableName, new ArrayList<Column>());  
	            	}	            	
           	}
           	else if ((element.equals("xs:element")) && (mapFlag == false) && (done == false)){
           		String keyName = findValue(lineStrArr,"name=");
           		String valueType = findValue(lineStrArr,"type=");
           		if (valueType.equals("tns:Map")){
           		  valueType = "xs:string";	
           		}
           		else if (valueType.equals("xs:anyType")){
           		  valueType = "xs:string";
           		}
           		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
           		keyNameValueTypePairs.put(keyName, valueType);
           		
           		Column col = new Column(keyName, valueType);
           		ArrayList columnArrList = tableNameAndColumns.get(tableName);
           		if (columnArrList.size() == 0){
           			col.setKey(true);
           		}
           		columnArrList.add(col);
           		tableNameAndColumns.put(tableName,columnArrList);
           	} 	
	    		//this is the END of the xs:complexType tag and indicates the end of this table's definition
           	else if (element.equals("/xs:complexType")){
           		done = true;
           	}
	    	} 	    	
	     }
	     
	     //HashMap<String, ArrayList<Column>> tableNameAndColumns
	     Iterator i = tableNameAndColumns.keySet().iterator();
	     while (i.hasNext()){
	       String tabName = (String)i.next();
	       System.out.println(tabName+" "+tableNameAndColumns.get(tabName));
	     }
	     
	     ObjectWriter ow = new ObjectMapper().writer();
	     String json = ow.writeValueAsString(tableNameAndColumns);
	     
	     System.out.println("json="+json);
	     
	     return json;
	     
    }
    
    //generates table from xsd
    @RequestMapping(value="/generateTable", method = RequestMethod.POST)
    public String generate(@RequestParam(value="input") String input) throws JAXBException{
    	
   	   Cluster cluster; 
   	   Session session;
   	   JSONObject jsonResult = new JSONObject();
   	   try {
		jsonResult.put("status","failure");
	   } catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
	   }
   	   
    	RestTemplate restTemplate = new RestTemplate();
    	System.out.println("input url="+input);
	    //String s = restTemplate.getForObject("https://bpm.capbpm.com:9443/webapi/ViewSchema.jsp?type=Customer&version=2064.374d42f7-af28-4f6d-a1c0-b34453c39b64T", String.class);
    	String s = restTemplate.getForObject(input, String.class);
    	
	    System.out.println("GenerateController generate() rest call returned s="+s);
	    /* s looks like:
	     * 
	     * <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://POAM" targetNamespace="http://POAM" elementFormDefault="qualified" attributeFormDefault="unqualified">

<xs:complexType name="Customer">
<xs:sequence>
<xs:element name="firstName" nillable="false" type="xs:string" minOccurs="0" maxOccurs="1"/>
<xs:element name="lastName" nillable="false" type="xs:string" minOccurs="0" maxOccurs="1"/>
<xs:element name="startDate" nillable="false" type="xs:dateTime" minOccurs="0" maxOccurs="1"/>
<xs:element name="isActive" nillable="false" type="xs:boolean" minOccurs="0" maxOccurs="1"/>
<xs:element name="age" nillable="false" type="xs:int" minOccurs="0" maxOccurs="1"/>
<xs:element name="accountBalance" nillable="false" type="xs:double" minOccurs="0" maxOccurs="1"/>
<xs:element name="alias" nillable="false" type="tns:Map" minOccurs="0" maxOccurs="1"/>
</xs:sequence>
</xs:complexType>

get line
remove < and >
split on space

	     */
	    
	    String tableName=null;
	    
	    String[] xsdArray = s.split("\\r?\\n");
	    Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();
	    
	    for (String line : xsdArray){
	    	//System.out.println("GenerateController generate() line="+line);
	    	line = line.trim();
	    	line = line.replace("<", "");
	    	line = line.replace(">", "");
	    	String[] lineStrArr = line.split(" ");
            for (String element : lineStrArr){
            	//System.out.println("GenerateController generate() element="+element);
            	if (element.indexOf("/xs:complexType") != -1){
            		/*
            		 * CREATE TABLE emp (
  empID int,
  deptID int,
  first_name varchar,
  last_name varchar
);
            		 */
            		String createTable = "CREATE TABLE "+tableName + "(";
            		String type = null;
            		
            		//System.out.println("GenerateController generate() Create table "+ tableName);
            		//System.out.println("GenerateController generate() Columns:");
            		Enumeration e = keyNameValueTypePairs.keys();
            		boolean execute = true;
            		boolean hasAtLeastOneColumn = false;
            		String primaryKey = null;
            		
            	    while (e.hasMoreElements()){
            	    	String k = (String)e.nextElement();
            	    	type = null;
            	    	String xsdType = keyNameValueTypePairs.get(k);
            	    	if (xsdType.compareTo("xs:string")==0){
            	    		type = "text";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:dateTime")==0){
            	    		type = "timestamp";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:boolean")==0){
            	    		type = "boolean";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:int")==0){
            	    		type = "int";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:double")==0){
            	    		type = "double";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	
            	    	if (type != null){
            	    	  System.out.println("GenerateController generate() colName="+k+" colType="+type);
            	    	  createTable = createTable + k + " " + type;
            	    	  
            	    	  if (primaryKey == null){
            	    		  primaryKey = k;
            	    	  }
              	    	  if (e.hasMoreElements()){
            	    		createTable = createTable +", ";
            	    	  }           	    	  
            	    	  
            	    	}
            	    	


            	    }
            	    createTable = createTable +", PRIMARY KEY (" + primaryKey +"));";
            	    if (execute && hasAtLeastOneColumn){
            	       System.out.println("GenerateController generate() NEW CREATE TABLE TO EXECUTE="+createTable);
            	       AccessDb ac = new AccessDb();
            	       if (ac.createTable(createTable)){
            	    	   try {
            	    		jsonResult = new JSONObject();
							jsonResult.put("status", "success");
						} catch (JSONException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
            	       }
            	    }
            	    keyNameValueTypePairs = new Hashtable<String,String>();
            	}
            	else if (element.indexOf("xs:complexType") != -1){
            		tableName=findValue(lineStrArr,"name=");
            		//System.out.println("GenerateController generate() tableName="+tableName);
            	}
            	else if (element.indexOf("xs:element") != -1){
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
            		keyNameValueTypePairs.put(keyName, valueType);
            	}
            }
	    }
	            
    	return jsonResult.toString();
    }
    
    //creates record in table give JSON input
    @RequestMapping(value="/create", method = RequestMethod.POST)
    public String create(@RequestParam(value="input") String input) {
        
    	// table:tablename, colval:[ {column:column, "type":type, value:value} ... ]
    	
    	// INSERT INTO Hollywood.NerdMovies (user_uuid, fan)
    	//  VALUES (cfd66ccc-d857-4e90-b1e5-df98a3d40cd6, 'johndoe')
    	
    	String jsonString = "";
    	AccessDb ac = new AccessDb();
    	JSONObject jsonReturnObject = new JSONObject();
    	String validator;
    	HashMap<String, String> columnNameTypeHM = new HashMap<String, String>();
    	
    	String columnsClause = "(";
    	String valuesClause = "(";
    	try {
			JSONObject jo = new JSONObject(input);
			String table = jo.getString("table").toLowerCase();
			
			ResultSet columnNameColumnTypeRS = ac.getTableMetadata(table);
			System.out.println("crudService GetController.read()");
			Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
		    while (rowIterator.hasNext()){
		    	Row r = rowIterator.next();
		    	System.out.println("columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
		    	validator = r.getString("validator");
		    	
			    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "double");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "int");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "text");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
			    }
		    
		    	
		    }
			
			JSONArray columnNameValueArr = jo.getJSONArray("colval");
			for (int idx=0; idx < columnNameValueArr.length(); idx++){
				JSONObject colValObj = columnNameValueArr.getJSONObject(idx);
				String column = colValObj.getString("column").toLowerCase();
				//String type = colValObj.getString("type");
				String type = columnNameTypeHM.get(column);
				String val = colValObj.getString("value");
				
				columnsClause = columnsClause + column;
				
				if (type.compareTo("timestamp") == 0){
					SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
					Date parsedDate = formatter.parse(val);
					valuesClause = valuesClause + parsedDate.getTime();
				}
				else if (type.compareTo("double") == 0){
					valuesClause = valuesClause + new Double(val).doubleValue();
				}else if (type.compareTo("int") == 0){
					valuesClause = valuesClause + new Integer(val).intValue();
				}else if (type.compareTo("text") == 0){
					valuesClause = valuesClause + "\'" + val + "\'";
				}
				else if (type.compareTo("boolean") == 0){
					valuesClause = valuesClause + val;
				}
				
				if (idx < columnNameValueArr.length()-1){
					columnsClause = columnsClause + ", ";
					valuesClause = valuesClause + ", ";
				}
			}
			
			columnsClause = columnsClause + ")";
			valuesClause = valuesClause + ")";
			
			String insertStatement = "insert into " + table + " " + columnsClause + " values " + valuesClause;
			System.out.println("insert " + insertStatement);
		    ResultSet rs = ac.run(insertStatement);
		    
		    if (rs != null){
		    	jsonReturnObject.put("status", "success");
		    }
		    else{
		    	jsonReturnObject.put("status", "fail");
		    }
		    
		    System.out.println("after insert rs="+rs.toString());
		} catch (JSONException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        return jsonReturnObject.toString();
        
    }
    
    @RequestMapping(value="/read", method = RequestMethod.POST)
    public String read(@RequestParam(value="input") String input) throws ParseException {
    	JSONArray ja = new JSONArray();
    	String comparisonClause = "";
    	//JSONArray colType = null;
    	String validator;
    	JSONArray postSelectComps;
    	//boolean include;
        ArrayList<Row> rowsToKeep = new ArrayList<Row>();    	
    	System.out.println("read input="+input);
    	AccessDb ac = new AccessDb();
    	
    	HashMap<String,String>columnNameTypeHM = new HashMap<String, String>();
    	
    	//{"table":"customer", "comps":[ {"column":"accountbalance" , "comp":"<", "type":"double", "value":"4.00" }]}
    	
    	try {
			JSONObject jo = new JSONObject(input);
			String table = jo.getString("table").toLowerCase();
			comparisonClause = this.generateComps(jo, table);
			postSelectComps = this.generatePostSelectComps(jo, table);
			if (postSelectComps.length() == 0){
			   postSelectComps = null;	
			}
			/*
			 * only = and IN supported
			 * 
			 * <, >, <=, >= must be done by
			 *    select * from .......
			 *    examining each result in ResultSet 
			 */
			System.out.println("read comparisonClause="+comparisonClause);
			
			ResultSet columnNameColumnTypeRS = ac.getTableMetadata(table);
			
			System.out.println("crudService GetController.read()");
			Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
		    while (rowIterator.hasNext()){
		    	Row r = rowIterator.next();
		    	System.out.println("columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
		    	validator = r.getString("validator");
		    	
			    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "double");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "int");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "text");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
			    }
		    
		    	
		    }
			
		    String selectStatement;
		    if (comparisonClause.equals("")){
				  selectStatement = "select * from " + table;		    	
		    }
		    else{
			  selectStatement = "select * from " + table + " where " + comparisonClause;
		    }
			System.out.println("read() select="+selectStatement);
			System.out.println("read() postSelectComps="+postSelectComps);
			ArrayList<Row> rs = ac.runReturnArrayList(selectStatement);

			if ((postSelectComps != null) && (postSelectComps.length() > 0)){
				for (int idx=0; idx<postSelectComps.length(); idx++ ){
					
					rowsToKeep = new ArrayList<Row>();
					
				    rowIterator = rs.iterator();
				    while (rowIterator.hasNext()){			    	
				    	Row r = rowIterator.next();
				    	if (postSelectComps.getJSONObject(idx) == null){
				    		rowsToKeep.add(r);
				    	}
				    	else{
			                if (secondaryFilter(r, postSelectComps.getJSONObject(idx), columnNameTypeHM )){
			                	rowsToKeep.add(r);
			                }
				    	}	                
				    }
				    rs = rowsToKeep;
				}				
			}
			else{
				rowsToKeep = rs;
				System.out.println("read() postSelectcomps null or 0 length");
			}

		    Iterator<Row> arrayListIt = rowsToKeep.iterator();
		    while (arrayListIt.hasNext()){
		    	Row r = arrayListIt.next();
		    	String rowString = "{";
		    	Iterator<String> columnNameTypeKeyIT = columnNameTypeHM.keySet().iterator();

		    	while (columnNameTypeKeyIT.hasNext()){

		    		String column = columnNameTypeKeyIT.next();
		    		String type = columnNameTypeHM.get(column);
		    	    	
					if (type.compareTo("timestamp") == 0){
						  Date result = r.getDate(column);
                          rowString = rowString + " \"" + column+"\":\"" + r.getDate(column) + "\"";
					}
					else if (type.compareTo("double") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getDouble(column) + "\"";
					}else if (type.compareTo("int") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getInt(column) + "\"";
					}else if (type.compareTo("text") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getString(column) + "\"";
					}
					else if (type.compareTo("boolean") == 0){
						rowString = rowString + " \"" + column+"\":\"" + r.getBool(column) + "\"";
					}						

					if (columnNameTypeKeyIT.hasNext()){
						rowString = rowString + ",";
					}
		    	}		    	
		    	rowString = rowString + "}";
		    	System.out.println("row="+rowString);
		    	JSONObject joResult = new JSONObject(rowString);
		    	System.out.println("joResult="+joResult);
		    	ja.put(joResult);		    	
		    }
		    
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	System.out.println("ja="+ja.toString());
    	return ja.toString();
    }
    
    public boolean secondaryFilter(Row r, JSONObject postSelectComps, HashMap<String, String> columnNameTypeHM  ) throws NumberFormatException, JSONException{
    	boolean include = true;
    	
    	System.out.println("secondaryFilter(); START postSelectComps="+postSelectComps.toString(2));
    	
    	String compType = postSelectComps.getString("comp");
    	
    	boolean equals=false, lessThan=false, lessThanEqualTo=false, greaterThan=false, greaterThanEqualTo=false, notEquals=false;
    	
    	if (compType.equals("=")){
    		equals=true;
    	}
    	else if (compType.equals("<")){
    		lessThan=true;
    	}
    	else if (compType.equals(">")){
    		greaterThan=true;
    	}
    	else if (compType.equals("<=")){
    		lessThanEqualTo = true;
    	}
    	else if (compType.equals(">=")){
    		greaterThanEqualTo = true;
    	}
    	else if (compType.equals("!=")){
    		notEquals = true;
    	}    	
    	
    	System.out.println("secondaryFilter() START lessThan="+lessThan+" lessThanEqualTo="+lessThanEqualTo+" greaterThan="+greaterThan+
    			" greaterThanEqualTo="+greaterThanEqualTo);
    	//for (int idx=0; idx<colType.length(); idx++){
    	Iterator<String> columnNameTypeKeyIT = columnNameTypeHM.keySet().iterator();
    	while (columnNameTypeKeyIT.hasNext()){
    		
    		String column = columnNameTypeKeyIT.next();
    		String type = columnNameTypeHM.get(column);
    	    System.out.println("read() column="+column);
    	    
    	    /*
    	     * for each column in table
    	     *    if column matches postSelectComps.field then do comparison
    	     */
    		if (postSelectComps.getString("field").equals(column)){
				if (type.compareTo("timestamp") == 0){
					  Date result = r.getDate(column);
					  Date limitVal = new Date((String)postSelectComps.get("limitvalue"));
					  System.out.println("secondaryFilter() comparing Dates result="+result+" limitVal="+limitVal+" comparison="+result.equals(limitVal));
                      if (equals){
						  if (result.equals(limitVal)){
							  //include = true;
						  }
						  else{
							  include = false;
						  }                   	  
                      }
                      else if (notEquals){
						  if (!result.equals(limitVal)){
							  //include = true;
						  }
						  else{
							  include = false;
						  }                   	  
                      }
                      else if (lessThan){
						  if (result.before(limitVal)){
							  //include = true;
						  }
						  else{
							  include = false;
						  }
					  }
					  else if (lessThanEqualTo){
						  if ((result.before(limitVal)) || result.equals(limitVal)){
							  //include = true;
						  }
						  else{
							  include = false;
						  }						  
					  }
					  else if (greaterThan){
						  if (result.after(limitVal)){
							  //include = true;
						  }
						  else{
							  include = false;
						  }						  
					  }
					  else if (greaterThanEqualTo){
						  if (result.after(limitVal) || result.equals(limitVal)){
							  //include = true;
						  }
						  else{
							  include = false;
						  }							  
					  }
				}
				else if (type.compareTo("double") == 0){
					Double result = r.getDouble(column);
					Double limitVal = new Double((String)postSelectComps.get("limitvalue"));
					System.out.println("secondaryFilter() result="+result+" limitVal="+limitVal+" comparison="+Double.min(result, limitVal));
                    if (equals){
						if (Double.compare(result, limitVal) == 0){
							//include = true;
						}
						else{
							include = false;
						}                    	
                    }
                    if (notEquals){
						if (Double.compare(result, limitVal) != 0){
							//include = true;
						}
						else{
							include = false;
						}                    	
                    }                    
                    else if (lessThan){
						if (Double.min(result, limitVal) == result){
							//include = true;
						}
						else{
							include = false;
						}
					}
					else if (lessThanEqualTo){
						if ((Double.min(result, limitVal) == result) || (result.doubleValue() == limitVal.doubleValue())){
							//include = true;
						}
						else{
							include = false;
						}						
					}
					else if (greaterThan){
						if (Double.max(result, limitVal) == result){
							//include = true;
						}
						else{
							include = false;
						}						  
					}
					else if (greaterThanEqualTo){
						if ((Double.max(result, limitVal) == result)  || (result.doubleValue() == limitVal.doubleValue())){
							//include = true;
						}
						else{
							include = false;
						}						  
					}
				}else if (type.compareTo("int") == 0){
					Integer result = r.getInt(column);
					Integer limitVal = new Integer((String)postSelectComps.get("limitvalue"));
					System.out.println("secondaryFilter() result="+result+" limitVal="+limitVal+" comparison="+(result.intValue() < limitVal.intValue()));
					if (equals){
						if (result.intValue() == limitVal.intValue()){
							//include = true;
						}
						else{
							include = false;
						}						
					}
					else if (notEquals){
						if (result.intValue() != limitVal.intValue()){
							//include = true;
						}
						else{
							include = false;
						}						
					}
					else if (lessThan){
						if (result.intValue() < limitVal.intValue()){
							//include = true;
						}
						else{
							include = false;
						}
					}
					else if (lessThanEqualTo){
						if ((result.intValue() < limitVal.intValue()) || (result.intValue() == limitVal.intValue())){
							//include = true;
						}
						else{
							include = false;
						}						
					}
					else if (greaterThan){
						if (result.intValue() > limitVal.intValue()){
							//include = true;
						}
						else{
							include = false;
						}						  
					}
					else if (greaterThanEqualTo){
						if ((result.intValue() > limitVal.intValue())  || (result.intValue() == limitVal.intValue())){
							//include = true;
						}
						else{
							include = false;
						}						  
					}
				}else if (type.compareTo("text") == 0){
					String result = r.getString(column);
					String limitVal = (String)postSelectComps.get("limitvalue");
					System.out.println("secondaryFilter() result="+result+" limitVal="+limitVal+" comparison="+result.compareTo(limitVal));
					if (equals){
						if (result.compareTo(limitVal) == 0){
							//include = true;
						}
						else{
							include = false;
						}						
					}
					else if (notEquals){
						if (result.compareTo(limitVal) != 0){
							//include = true;
						}
						else{
							include = false;
						}						
					}
					else if (lessThan){
						if (result.compareTo(limitVal) <= -1){
							//include = true;
						}
						else{
							include = false;
						}
					}
					else if (lessThanEqualTo){
						if (result.compareTo(limitVal) <= 0){
							//include = true;
						}
						else{
							include = false;
						}						
					}
					else if (greaterThan){
						if (result.compareTo(limitVal) > 0){
							//include = true;
						}
						else{
							include = false;
						}						  
					}
					else if (greaterThanEqualTo){
						if (result.compareTo(limitVal) >= 0){
							//include = true;
						}
						else{
							include = false;
						}						  
					}
				}
				else if (type.compareTo("boolean") == 0){
					Boolean result = r.getBool(column);
					Boolean limitVal = postSelectComps.getBoolean("limitvalue");
					if (equals){
						if (result.equals(limitVal)){
							//include = true;
						}
						else{
							include = false;
						}
					}
					else if (notEquals){
						if (!result.equals(limitVal)){
							//include = true;
						}
						else{
							include = false;
						}
					}
					System.out.println("Boolean field no comparison");
                    //include = true;
				}						
				
    		}
    		else{
                //include = true;
    			System.out.println("read(); column="+column+" not in postSelectComps="+postSelectComps.toString()); 
    		}

    	}    	
    	
    	return include;
    }
    @RequestMapping(value="/update", method = RequestMethod.POST)
    public String update(@RequestParam(value="input") String input) {
    	
    	/*
    	 * UPDATE users
             SET name = 'John Smith', email = 'jsmith@cassie.com'
             WHERE user_uuid = 88b8fd18-b1ed-4e96-bf79-4280797cba80;
    	 */
    	
    	// table:tablename, colval:[ {column:column, "type":type, value:value} ... ], 
    	//      "comps":[ {"column":"accountbalance" , "comp":"<", "type":"double", "value":"4.00" }]} 
    	
    	JSONArray ja = new JSONArray();
    	String comparisonClause = "";
    	String updateClause = "SET ";
    	JSONObject jsonReturnObject = new JSONObject();
    	
    	AccessDb ac = new AccessDb();
    	HashMap<String,String>columnNameTypeHM = new HashMap<String, String>();
    	String validator;
    	
    	try {
			JSONObject jo = new JSONObject(input);
			String table = jo.getString("table").toLowerCase();

			ResultSet columnNameColumnTypeRS = ac.getTableMetadata(table);
			System.out.println("crudService GetController.read()");
			Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
		    while (rowIterator.hasNext()){
		    	Row r = rowIterator.next();
		    	System.out.println("columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
		    	validator = r.getString("validator");
		    	
			    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "double");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "int");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "text");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
			    }
		    
		    	
		    }
		    
			JSONArray updateColumnArr = jo.getJSONArray("colval");
			for (int idx=0; idx < updateColumnArr.length(); idx++){
				JSONObject colValObj = updateColumnArr.getJSONObject(idx);
				String column = colValObj.getString("column").toLowerCase();
				//String type = colValObj.getString("type");
				String type = columnNameTypeHM.get(column);
				String val = colValObj.getString("value");
				
				//updateClause = updateClause + column;
				
				if (type.compareTo("timestamp") == 0){
					SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
					Date parsedDate = formatter.parse(val);
					updateClause = updateClause + column + "=" + parsedDate.getTime();
				}
				else if (type.compareTo("double") == 0){
					updateClause = updateClause + column + "=" + new Double(val).doubleValue();
				}else if (type.compareTo("int") == 0){
					updateClause = updateClause + column + "=" + new Integer(val).intValue();
				}else if (type.compareTo("text") == 0){
					updateClause = updateClause + column + "=" + "\'" + val + "\'";
				}
				else if (type.compareTo("boolean") == 0){
					updateClause = updateClause + column + "=" + val;
				}
				
				if (idx < updateColumnArr.length()-1){
					updateClause = updateClause + ", ";
				}
			}		
			
			JSONArray compArr = jo.getJSONArray("comps");
			for (int idx=0; idx < compArr.length(); idx++){
				JSONObject compObj = compArr.getJSONObject(idx);
				String key = compObj.getString("column").toLowerCase();
				String comp = compObj.getString("comp");
				//String type = compObj.getString("type");
				String type = columnNameTypeHM.get(key);
				String val = compObj.getString("value");
				

				if (type.compareTo("timestamp") == 0){
					SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
					Date parsedDate = formatter.parse(val);
					comparisonClause = comparisonClause + key + " " + comp + " " + parsedDate.getTime();
				}
				else if (type.compareTo("double") == 0){
					comparisonClause = comparisonClause + key + " " + comp + " " + new Double(val).doubleValue();
				}else if (type.compareTo("int") == 0){
					comparisonClause = comparisonClause + key + " " + comp + " " + new Integer(val).intValue();
				}else if (type.compareTo("text") == 0){
					comparisonClause = comparisonClause + key + " " + comp + " " + "\'" + val + "\'";
				}
				else if (type.compareTo("boolean") == 0){
				    comparisonClause = comparisonClause + "=" + val;
				}				
				//comparisonClause = key + comp;
			}
			
			String updateStatement = "update " + table + " " + updateClause + " where " + comparisonClause;
			
			System.out.println("update="+updateStatement);
		    ResultSet rs = ac.run(updateStatement);
		    
		    if (rs != null){
		    	jsonReturnObject.put("status", "success");
		    }
		    else{
		    	jsonReturnObject.put("status", "fail");
		    }		    
		    
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return jsonReturnObject.toString();
    }
    
    @RequestMapping(value="/delete", method = RequestMethod.POST)
    public String delete(@RequestParam(value="input") String input) {
    	
    	// table:tablename, comps:[ {key:key, comp:comparison, val:value}, ... ]
    	JSONArray ja = new JSONArray();
    	String comparisonClause = "";
    	JSONObject jsonReturnObject = new JSONObject();
    	String validator;
    	HashMap<String,String> columnNameTypeHM = new HashMap<String,String>();
    	AccessDb ac = new AccessDb();
    	
    	try {
			JSONObject jo = new JSONObject(input);
			String table = jo.getString("table").toLowerCase();
			
			ResultSet columnNameColumnTypeRS = ac.getTableMetadata(table);
			System.out.println("crudService GetController.read()");
			Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
		    while (rowIterator.hasNext()){
		    	Row r = rowIterator.next();
		    	System.out.println("columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
		    	validator = r.getString("validator");
		    	
			    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "double");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "int");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
			    	columnNameTypeHM.put(r.getString("column_name"), "text");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
			    }
			    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
			    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
			    }
		    
		    	
		    }
			
			JSONArray compArr = jo.getJSONArray("comps");
			for (int idx=0; idx < compArr.length(); idx++){
				JSONObject compObj = compArr.getJSONObject(idx);
				String key = compObj.getString("column").toLowerCase();
				//String type = compObj.getString("type");
				String type = columnNameTypeHM.get(key);
				String comp = compObj.getString("comp");
				String val = compObj.getString("value");
				
				if (type.compareTo("timestamp") == 0){
					SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
					Date parsedDate = formatter.parse(val);
					comparisonClause = comparisonClause + key + " " + comp + " " + parsedDate.getTime();					
					//comparisonClause = comparisonClause + key + " " + comp + "\'" + val + "\'";
				}
				else if (type.compareTo("double") == 0){
					comparisonClause = comparisonClause + key + " " + comp + " " + new Double(val).doubleValue();
				}else if (type.compareTo("int") == 0){
					comparisonClause = comparisonClause + key + " " + comp + " " + new Integer(val).intValue();
				}else if (type.compareTo("text") == 0){
					comparisonClause = comparisonClause + key + " " + comp + " " + "\'" + val + "\'";
				}
				else if (type.compareTo("boolean") == 0){
				    comparisonClause = comparisonClause + "=" + val;
				}
				
			}
			
			String deleteStatement = "delete from " + table + " where " + comparisonClause;
			System.out.println("delete="+deleteStatement);
		    ResultSet rs = ac.run(deleteStatement);
		    
		    if (rs != null){
		    	jsonReturnObject.put("status", "success");
		    }
		    else{
		    	jsonReturnObject.put("status", "fail");
		    }
		    
		    
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return jsonReturnObject.toString();
    }

    //xs:complexType name="Customer"
    public String findValue(String[] arr, String key){
    	//System.out.println("findValue arr="+arr+" key="+key);
    	String value=null;
    	for (int idx = 1; idx < arr.length; idx++){
    		if (arr[idx].indexOf(key) != -1){
    			String[] keyValueArr=arr[idx].split("=");
    			value = keyValueArr[1];
    			value = value.replace("\"", "");
    			break;
    		}
    	}
    	return value;
    	
    }
    
    //generateTable given JSON rep of Object
    @RequestMapping(value="/generateTable2", method = RequestMethod.POST)
    public String generate2(@RequestParam(value="name") String name,
    		                @RequestParam(value="primaryKey") String primaryKey,
    		                @RequestParam(value="input") String jsonStringInput) throws JAXBException{
    	
   	   Cluster cluster; 
   	   Session session;
   	   
   	// INSERT INTO Hollywood.NerdMovies (user_uuid, fan)
   	//  VALUES (cfd66ccc-d857-4e90-b1e5-df98a3d40cd6, 'johndoe')
   	   
   	   HashMap<String,Object> keyValuePairs = new HashMap<String, Object>();
   	   
   	   JSONObject jsonResult = new JSONObject();
   	   try {
		jsonResult.put("status","failure");
	   } catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
	   }

   	   
   	   try {
      	    JSONObject jo = new JSONObject(jsonStringInput);
    	    
    	    String tableName=null;
    	    
    	    String createTable = "create table " + name + "(";
    	    String insert = "insert into " + name;
    	    /*
    		 * CREATE TABLE emp (
                           empID int,
                           deptID int,
                           first_name varchar,
                           last_name varchar
                );
             */
    	    
    	    Iterator<String> iterator = jo.keys();
    	    while (iterator.hasNext()){
    	    	String key = iterator.next();
    	    	Object value = jo.get(key);
                if (value instanceof Integer){
    	    		createTable = createTable + key + " bigint";
    	    		keyValuePairs.put(key,((Integer)value).intValue());
    	    	}
    	    	else if (value instanceof String){
    	    		createTable = createTable + key + " text";
    	    		keyValuePairs.put(key,(String)value);
    	    	}
    	    	else if (value instanceof Boolean){
    	    		createTable = createTable + key + " boolean";
    	    		keyValuePairs.put(key,((Boolean)value).booleanValue());
    	    	}
    	    	
                if (key.compareTo(primaryKey) == 0){
                	createTable = createTable +" primary key";
                }
    	    	if (iterator.hasNext()){
    	    		createTable = createTable + ", ";
    	    	}
    	    }
    	    
    	    createTable = createTable + ")";
    	    System.out.println("createTable="+createTable);
    	    
 	        AccessDb ac = new AccessDb();
 	        if (ac.createTable(createTable)){
 	        	
 	        	String colNamesString = "(";
 	        	String valuesString = " values (";
 	        	
 	        	Iterator<String> keyNames = keyValuePairs.keySet().iterator();
 	        	while (keyNames.hasNext()){
 	        		String key = keyNames.next();
 	        		colNamesString = colNamesString + key;
 	        		if (keyValuePairs.get(key) instanceof String){
 	        			valuesString = valuesString + "'" + keyValuePairs.get(key) + "'";
  	        		}
 	        		else{
 	        			valuesString = valuesString+keyValuePairs.get(key);
 	        		}
 	        		
 	        		if (keyNames.hasNext()){
 	        			colNamesString = colNamesString + ", ";
 	        			valuesString = valuesString + ", ";
 	        		}
 	        	}
 	        	colNamesString = colNamesString + ")";
 	        	valuesString = valuesString + ")";
 	        	
 	        	insert = insert + colNamesString + valuesString;
 	        	System.out.println("insert="+insert);
 	        	ResultSet rs = ac.run(insert);
 			    
 			    if (rs != null){
 			    	jsonResult = new JSONObject();
					jsonResult.put("status", "success");
 			    }
 			    else{
 			    	jsonResult = new JSONObject();
 			    	jsonResult.put("status", "fail");
 			    }
 			    
 	        } 
 	        else{
			    	jsonResult = new JSONObject();
			    	jsonResult.put("status", "fail"); 	        	
 	        }
 	        
    	    
	   } catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
	   }   	   

	    
	    
	    	            
    	return jsonResult.toString();
    }  
    
    @RequestMapping(value="/generateBean", method = RequestMethod.POST)
    public String generateBean(@RequestParam(value="input") String input) throws JAXBException{
    	
   	   Cluster cluster; 
   	   Session session;
   	   JSONObject jsonResult = new JSONObject();
   	   try {
		jsonResult.put("status","failure");
	   } catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
	   }
   	   
    	RestTemplate restTemplate = new RestTemplate();
    	System.out.println("input url="+input);
	    //String s = restTemplate.getForObject("https://bpm.capbpm.com:9443/webapi/ViewSchema.jsp?type=Customer&version=2064.374d42f7-af28-4f6d-a1c0-b34453c39b64T", String.class);
    	String s = restTemplate.getForObject(input, String.class);
    	
	    System.out.println("GenerateController generate() rest call returned s="+s);
	    /* s looks like:
	     * 
	     * <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:tns="http://POAM" targetNamespace="http://POAM" elementFormDefault="qualified" attributeFormDefault="unqualified">

<xs:complexType name="Customer">
<xs:sequence>
<xs:element name="firstName" nillable="false" type="xs:string" minOccurs="0" maxOccurs="1"/>
<xs:element name="lastName" nillable="false" type="xs:string" minOccurs="0" maxOccurs="1"/>
<xs:element name="startDate" nillable="false" type="xs:dateTime" minOccurs="0" maxOccurs="1"/>
<xs:element name="isActive" nillable="false" type="xs:boolean" minOccurs="0" maxOccurs="1"/>
<xs:element name="age" nillable="false" type="xs:int" minOccurs="0" maxOccurs="1"/>
<xs:element name="accountBalance" nillable="false" type="xs:double" minOccurs="0" maxOccurs="1"/>
<xs:element name="alias" nillable="false" type="tns:Map" minOccurs="0" maxOccurs="1"/>
</xs:sequence>
</xs:complexType>

get line
remove < and >
split on space

	     */
	    
	    String className=null;
	    
	    ArrayList<String> programText = new ArrayList<String>();
	    
	    
	    ArrayList<String> getterText = new ArrayList<String>();
	    ArrayList<String> setterText = new ArrayList<String>();
	    
	    String[] xsdArray = s.split("\\r?\\n");
	    Hashtable<String,String> keyNameValueTypePairs = new Hashtable<String,String>();
	    
	    for (String line : xsdArray){
	    	//System.out.println("GenerateController generate() line="+line);
	    	line = line.trim();
	    	line = line.replace("<", "");
	    	line = line.replace(">", "");
	    	String[] lineStrArr = line.split(" ");
            for (String element : lineStrArr){
            	//System.out.println("GenerateController generate() element="+element);
            	if (element.indexOf("/xs:complexType") != -1){
            		/*
            		 * CREATE TABLE emp (
  empID int,
  deptID int,
  first_name varchar,
  last_name varchar
);
            		 */
            		//String createTable = "CREATE TABLE "+tableName + "(";
            		programText.add("package com.capbpm;");
            		programText.add(" ");
            		programText.add("import javax.persistence.Entity;");
            		programText.add("import javax.persistence.Id;");
            		programText.add("import javax.persistence.Table;");
            		programText.add("import javax.persistence.Column;");
    	    		programText.add("import java.util.Date;");
    	    	  
            		programText.add(" ");
            		programText.add("@Entity");
            		programText.add("@Table(name = \""+className.toLowerCase()+"\", schema = \"showcase@cassandra_pu\")"); 
            		programText.add("public class "+ className + "{");
            		programText.add(" ");
    	    		programText.add(" @Id");
    	    		programText.add(" private long id;");
      	    	  programText.add(" ");
      	    	  programText.add("  public long getId(){");
      	    	  programText.add("    return this.id;");
      	    	  programText.add("  }");
      	    	  programText.add(" ");
      	    	  
      	    	  programText.add("  public void setId( long id ){");
      	    	  programText.add("    this.id=id;");
      	    	  programText.add("  }");
      	    	  programText.add(" ");
      	    	  
            		String type = null;
            		
            		//System.out.println("GenerateController generate() Create table "+ tableName);
            		//System.out.println("GenerateController generate() Columns:");
            		Enumeration e = keyNameValueTypePairs.keys();
            		boolean execute = true;
            		boolean hasAtLeastOneColumn = false;
            		String primaryKey = null;
            		
            	    while (e.hasMoreElements()){
            	    	String k = (String)e.nextElement();
            	    	String firstCharInKupperCase = k.substring(0, 1).toUpperCase() + k.substring(1);
            	    	type = null;
            	    	String xsdType = keyNameValueTypePairs.get(k);
            	    	if (xsdType.compareTo("xs:string")==0){
            	    		type = "String";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:dateTime")==0){
            	    		type = "Date";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:boolean")==0){
            	    		type = "boolean";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:int")==0){
            	    		type = "int";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	else if (xsdType.compareTo("xs:double")==0){
            	    		type = "double";
            	    		hasAtLeastOneColumn = true;
            	    	}
            	    	
            	    	if (type != null){
            	    	  System.out.println("GenerateController generate() colName="+k+" colType="+type);
            	    	  //createTable = createTable + k + " " + type;
            	    	  
            	    	  programText.add("@Column(name=\""+k+"\")"); 
            	    	  programText.add("  private "+type+" "+k+";");
            	    	  programText.add(" ");
            	    	  programText.add("  public "+type+" get"+firstCharInKupperCase+"(){");
            	    	  programText.add("    return this."+k+";");
            	    	  programText.add("  }");
            	    	  programText.add(" ");
            	    	  
            	    	  programText.add("  public void set"+firstCharInKupperCase+"( "+type+" "+k+"){");
            	    	  programText.add("    this."+k+" = "+k+";");
            	    	  programText.add("  }");
            	    	  programText.add(" ");
            	    	  
              	    	  if (e.hasMoreElements()){
            	    		//createTable = createTable +", ";
            	    	  }
              	    	  else{
              	    		  programText.add("}");
              	    	  }
            	    	  
            	    	}
            	    	


            	    }
            	    //createTable = createTable +", PRIMARY KEY (" + primaryKey +"));";
            	    if (execute && hasAtLeastOneColumn){
            	    	
            	       
            	       try {
            	    	   File f = new File(System.getProperty("java.io.tmpdir")+className+".java");   
						   PrintStream ps = new PrintStream(f);
	            	       for (String text:programText){
	            	    	   ps.println(text);
	            	       }
	            	       ps.flush();
	            	       ps.close();
	            	       
	            	   	   jsonResult = new JSONObject();
	            	   	   try {
	            			jsonResult.put("status","success");
	            		   } catch (JSONException e2) {
	            				// TODO Auto-generated catch block
	            				e2.printStackTrace();
	            		   }
	            	   	   
					   } catch (FileNotFoundException e1) {
						   // TODO Auto-generated catch block
						   e1.printStackTrace();
					   }

            	       //this is where we write out programText to file
            	       /*System.out.println("GenerateController generate() NEW CREATE TABLE TO EXECUTE="+createTable);
            	       AccessCassandra ac = new AccessCassandra();
            	       if (ac.createTable(createTable)){
            	    	   try {
            	    		jsonResult = new JSONObject();
							jsonResult.put("status", "success");
						} catch (JSONException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
            	       }*/
            	    }
            	    keyNameValueTypePairs = new Hashtable<String,String>();
            	}
            	else if (element.indexOf("xs:complexType") != -1){
            		//tableName=findValue(lineStrArr,"name=");
            		className=findValue(lineStrArr,"name=");
            		//System.out.println("GenerateController generate() tableName="+tableName);
            	}
            	else if (element.indexOf("xs:element") != -1){
            		String keyName = findValue(lineStrArr,"name=");
            		String valueType = findValue(lineStrArr,"type=");
            		//System.out.println("GenerateController generate() tableName="+tableName+" keyName="+keyName+" valueType="+valueType);
            		keyNameValueTypePairs.put(keyName, valueType);
            	}
            }
	    }
	            
    	return jsonResult.toString();
    }

    /*
     *     @RequestMapping("/generateTable2")
    public String generate2(@RequestParam(value="name") String name,
    		                @RequestParam(value="primaryKey") String primaryKey,
    		                @RequestParam(value="input") String jsonStringInput) throws JAXBException{
     */
    @RequestMapping(value="/generateBean2", method = RequestMethod.POST)
    public String generateBean2(@RequestParam(value="name") String name,
                                @RequestParam(value="primaryKey") String primaryKey,
                                @RequestParam(value="input") String jsonStringInput) throws JAXBException{
    	
   	   Cluster cluster; 
   	   Session session;
   	   JSONObject jsonResult = new JSONObject();
	   String className=name;
	    
	   ArrayList<String> programText = new ArrayList<String>();
	     
	   ArrayList<String> getterText = new ArrayList<String>();
	   ArrayList<String> setterText = new ArrayList<String>();
	    
		programText.add("package com.capbpm;");
		programText.add(" ");
		programText.add("import javax.persistence.Entity;");
		programText.add("import javax.persistence.Id;");
		programText.add("import javax.persistence.Table;");
		programText.add("import javax.persistence.Column;");
		programText.add("import java.util.Date;");
	  
		programText.add(" ");
		programText.add("@Entity"); 
		programText.add("@Table(name = \""+className.toLowerCase()+"\", schema = \"showcase@cassandra_pu\")"); 
		programText.add("public class "+ className + "{");
		programText.add(" ");
		programText.add(" @Id");
		programText.add(" private long id;");
 	  programText.add(" ");
 	  programText.add("  public long getId(){");
 	  programText.add("    return this.id;");
 	  programText.add("  }");
 	  programText.add(" ");
 	  
 	  programText.add("  public void setId( long id ){");
 	  programText.add("    this.id=id;");
 	  programText.add("  }");
 	  programText.add(" ");
 	  
   	   String input = jsonStringInput;
   	   try {
		jsonResult.put("status","failure");
	   } catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
	   }
   	   /*new code form generateTable2 */
   	   try {
     	    JSONObject jo = new JSONObject(jsonStringInput);
   	    
   	        className = name;
   	        

   	    String type = "";
   	    Iterator<String> iterator = jo.keys();
   	    while (iterator.hasNext()){
   	    	String key = iterator.next();
   	    	String firstCharInKupperCase = key.substring(0, 1).toUpperCase() + key.substring(1);
   	    	Object value = jo.get(key);
               if (value instanceof Integer){
   	    		type = "Integer";
   	    	}
   	    	else if (value instanceof String){
   	    		type = "String";
   	    	}
   	    	else if (value instanceof Boolean){
   	    		type = "Boolean";
   	    	}
   	    	
              programText.add("@Column(name=\""+key+"\")"); 
	    	  programText.add("  private "+type+" "+key+";");
	    	  programText.add(" ");
	    	  programText.add("  public "+type+" get"+firstCharInKupperCase+"(){");
	    	  programText.add("    return this."+key+";");
	    	  programText.add("  }");
	    	  programText.add(" ");
	    	  
	    	  programText.add("  public void set"+firstCharInKupperCase+"( "+type+" "+key+"){");
	    	  programText.add("    this."+key+" = "+key+";");
	    	  programText.add("  }");
	    	  programText.add(" ");
   	    }
   	    
   	    programText.add("}");
   	    
	       try {
	    	   File f = new File(System.getProperty("java.io.tmpdir")+className+".java");   
			   PrintStream ps = new PrintStream(f);
    	       for (String text:programText){
    	    	   ps.println(text);
    	       }
    	       ps.flush();
    	       ps.close();
    	       
    	   	   jsonResult = new JSONObject();

    			jsonResult.put("status","success");

    	   	   
		   } catch (FileNotFoundException e1) {
			   // TODO Auto-generated catch block
			   e1.printStackTrace();
		   }
	       

        
   	   }
   	   catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
	   }
   	   
   	   return jsonResult.toString();
    }
    
    private String generateComps(JSONObject jo, String tableName) throws JSONException, ParseException{
    	HashMap<String, String> columnNameTypeHM = new HashMap<String, String>();
    	HashMap<String, Boolean> columnNameIsKeyHM = new HashMap<String, Boolean>();
    	String comparisonClause = "";
    	String val;
    	boolean usedKey=false;
    	JSONArray jsonValArr;
    	
    	AccessDb adb = new AccessDb();
    	ResultSet columnNameColumnTypeRS = adb.getTableMetadata(tableName);
		
		System.out.println("crudService GetController.read()");
		Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
	    while (rowIterator.hasNext()){
	    	Row r = rowIterator.next();
	    	System.out.println("columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
	    	String validator = r.getString("validator");
	    	
	    	columnNameIsKeyHM.put(r.getString("column_name"), r.getString("type").trim().equals("partition_key") ? new Boolean(true): new Boolean(false));
	    	
		    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
		    	columnNameTypeHM.put(r.getString("column_name"), "double");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
		    	columnNameTypeHM.put(r.getString("column_name"), "int");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
		    	columnNameTypeHM.put(r.getString("column_name"), "text");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
		    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
		    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
		    }
	    
	    	
	    }
	    
		JSONArray compArr = jo.getJSONArray("comps");
		System.out.println("generateComps() compArr=");
		System.out.println(compArr.toString(2));
		for (int idx=0; idx < compArr.length(); idx++){
			JSONObject compObj = compArr.getJSONObject(idx);
			String key = compObj.getString("column").toLowerCase();
			//String type = compObj.getString("type");
			String type = columnNameTypeHM.get(key);
			String comp = compObj.getString("comp");
						
			System.out.println("generateComps() key="+key);
			System.out.println("generateComps() comp="+comp);
			
			if (columnNameIsKeyHM.get(key)){
				if (idx != 0){
					comparisonClause += " and ";
				}
				if (comp.equals("=")){
				    if (columnNameIsKeyHM.get(key)){
				    	usedKey=true;
				    }
					val = compObj.getString("value");
					System.out.println("generateComps() val="+val);
					if (type.compareTo("timestamp") == 0){
						System.out.println("generateComps() converting date val=["+val.trim()+"]");
						SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
						Date parsedDate = formatter.parse(val.trim());
						comparisonClause = comparisonClause + key + " " + comp + " " + parsedDate.getTime();					
						//comparisonClause = comparisonClause + key + " " + comp + "\'" + val + "\'";
					}
					else if (type.compareTo("double") == 0){
						comparisonClause = comparisonClause + key + " " + comp + " " + new Double(val).doubleValue();
					}else if (type.compareTo("int") == 0){
						comparisonClause = comparisonClause + key + " " + comp + " " + new Integer(val).intValue();
					}else if (type.compareTo("text") == 0){
						comparisonClause = comparisonClause + key + " " + comp + " " + "\'" + val + "\'";
					}
					else if (type.compareTo("boolean") == 0){
					    comparisonClause = comparisonClause + "=" + val;
					}
				}
				else if (comp.equals("in")){
				    if (columnNameIsKeyHM.get(key)){
				    	usedKey=true;
				    }
					//comps:[ "column":"xxxxx", "comp":"in", "value":["val1", "val2", "val3"] ]
			        jsonValArr = compObj.getJSONArray("value");
			        comparisonClause += key + " " + comp + "(";
			        
			        for (int idx2=0; idx2<jsonValArr.length(); idx2++){
			        	val = jsonValArr.getString(idx2);
			        	System.out.println("generateComps() val="+val);
			        	if (idx2 != 0){
			        		comparisonClause += ", ";
			        	}
						if (type.compareTo("timestamp") == 0){
							SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
							Date parsedDate = formatter.parse(val);
							comparisonClause = comparisonClause + parsedDate.getTime();					
						}
						else if (type.compareTo("double") == 0){
							comparisonClause = comparisonClause + new Double(val).doubleValue();
						}else if (type.compareTo("int") == 0){
							comparisonClause = comparisonClause + new Integer(val).intValue();
						}else if (type.compareTo("text") == 0){
							comparisonClause = comparisonClause + "\'" + val + "\'";
						}
						else if (type.compareTo("boolean") == 0){
						    comparisonClause = comparisonClause + val;
						}		        	
			        }
			        
			        comparisonClause += ")";
				}
				else if (comp.equals("<")){ 

					comparisonClause = "";
				}
				else if (comp.equals(">")){ 
	        
				}				
			}
			else{
				System.out.println("generateComps() "+key+" is not primary key so skip it");
			}

			
		}   	
    	
		if (usedKey == false){
			System.out.println("generateComps() comparisonClause="+comparisonClause+" does not contain a key so wiping out");			
			comparisonClause="";
		}
		System.out.println("generateComps comparisonClause="+comparisonClause);
    	return comparisonClause;
    	
    }
    
    private JSONArray generatePostSelectComps(JSONObject jo, String tableName) throws JSONException, ParseException{
    	HashMap<String, String> columnNameTypeHM = new HashMap<String, String>();
    	String comparisonClause = "";
    	String val;
    	JSONArray jsonValArr;
    	JSONObject resultJo;
    	JSONArray resultJa=new JSONArray();
    	AccessDb adb = new AccessDb();
    	ResultSet columnNameColumnTypeRS = adb.getTableMetadata(tableName);
		
		System.out.println("crudService GetController.generatePostSelectComps() START");
		Iterator<Row> rowIterator = columnNameColumnTypeRS.iterator();
	    while (rowIterator.hasNext()){
	    	Row r = rowIterator.next();
	    	System.out.println("columnName="+r.getString("column_name")+" validator="+r.getString("validator"));
	    	String validator = r.getString("validator");
	    	
		    if (validator.contentEquals("org.apache.cassandra.db.marshal.DoubleType")){
		    	columnNameTypeHM.put(r.getString("column_name"), "double");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.Int32Type")){
		    	columnNameTypeHM.put(r.getString("column_name"), "int");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.UTF8Type")){
		    	columnNameTypeHM.put(r.getString("column_name"), "text");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.BooleanType")){
		    	columnNameTypeHM.put(r.getString("column_name"), "boolean");
		    }
		    else if (validator.contentEquals("org.apache.cassandra.db.marshal.TimestampType")){
		    	columnNameTypeHM.put(r.getString("column_name"), "timestamp");
		    }
	    
	    	
	    }
	    
		JSONArray compArr = jo.getJSONArray("comps");
		for (int idx=0; idx < compArr.length(); idx++){
			resultJo=new JSONObject();
			JSONObject compObj = compArr.getJSONObject(idx);
			String key = compObj.getString("column").toLowerCase();
			//String type = compObj.getString("type");
			String type = columnNameTypeHM.get(key);
			String comp = compObj.getString("comp");
						
			System.out.println("generatePostSelectComps() key="+key);
			System.out.println("generatePostSelectComps() type="+type);
			if (comp.equals("=")){
                //jsonValArr = compObj.getJSONArray("value");
				//val = jsonValArr.getString(0);
				val = compObj.getString("value");
				System.out.println("generatePostSelectComps() val="+val);
		        comparisonClause += "token("+key + ") < token('";
		        
		        resultJo.put("field", key);
		        resultJo.put("comp", "=");
		        
		        resultJo.put("limitvalue", val); 
		        
				comparisonClause = "";				
			}
			else if (comp.equals("!=")){
				val = compObj.getString("value");
				System.out.println("generatePostSelectComps() val="+val);
		        comparisonClause += "token("+key + ") < token('";
		        
		        resultJo.put("field", key);
		        resultJo.put("comp", "!=");
		        
		        resultJo.put("limitvalue", val); 
		        
				comparisonClause = "";				
			}
			else if (comp.equals("<")){ 
                jsonValArr = compObj.getJSONArray("value");
				val = jsonValArr.getString(0);
				System.out.println("generatePostSelectComps() val="+val);
		        comparisonClause += "token("+key + ") < token('";
		        
		        resultJo.put("field", key);
		        resultJo.put("comp", "<");
		        resultJo.put("limitvalue", val); 

				comparisonClause = "";
			}
            else if (comp.equals("<=")){ 
                jsonValArr = compObj.getJSONArray("value");
				val = jsonValArr.getString(0);
				System.out.println("generatePostSelectComps() val="+val);
		        comparisonClause += "token("+key + ") < token('";
		        
		        resultJo.put("field", key);
		        resultJo.put("comp", "<=");
		        resultJo.put("limitvalue", val); 

				comparisonClause = "";
			}
			else if (comp.equals(">")){ 
				jsonValArr = compObj.getJSONArray("value");
				val = jsonValArr.getString(0);
				System.out.println("generatePostSelectComps() val="+val);
		        comparisonClause += key + " in (";
		        
		        resultJo.put("field", key);
		        resultJo.put("comp", ">");
		        resultJo.put("limitvalue", val);
	        
			}
			else if (comp.equals(">=")){ 
				jsonValArr = compObj.getJSONArray("value");
				val = jsonValArr.getString(0);
				System.out.println("generatePostSelectComps() val="+val);
		        comparisonClause += key + " in (";
		        
		        resultJo.put("field", key);
		        resultJo.put("comp", ">=");
		        resultJo.put("limitvalue", val); 
        
			}			
			else{
				resultJo = null;
			}
			if (resultJo != null){
				resultJa.put(resultJo);
			}
			
		}   	
		/*if (resultJo == null){
		  System.out.println("crudService GetController.generatePostSelectComps() resultJo==NULL no secondary filtering will be done");
		}
		else{
		  System.out.println("crudService GetController.generatePostSelectComps() resultJo="+resultJo.toString(2));
		}
    	return resultJo;*/

	    return resultJa;
    	
    }
}


