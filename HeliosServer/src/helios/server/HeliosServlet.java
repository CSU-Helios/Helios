package helios.server;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import helios.javadriver.Javamongo;

 
@WebServlet("/helios")
public class HeliosServlet extends HttpServlet {

	private Javamongo javamongo;

	private static final long serialVersionUID = 1L;
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		doGetOrPost(request, response);
	}
	
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		doGetOrPost(request, response);
	}

	/**
	 * This method handles both GET and POST requests.
	 * @param request
	 * @param response
	 * @throws IOException
	 * @throws ServletException
	 */
	private void doGetOrPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		StringBuffer sb = new StringBuffer();
		String line = null;
		try {
			BufferedReader reader = request.getReader();
			while ((line = reader.readLine()) != null)
				sb.append(line);
		} catch (IOException ie) {
			System.out.println("Failed to retrieve data from server request");
		}
		JsonReader jsonReader = Json.createReader(new StringReader(sb.toString()));
		JsonObject jsonObj = jsonReader.readObject();
		jsonReader.close();
		
		this.connectToJavaMongo(jsonObj);

		response.setContentType("text/x-json;charset=UTF-8");           
        response.setHeader("Cache-Control", "no-cache");
        
        JsonArray recArr = javamongo.queryCollectionRetJson(jsonObj);
		for (JsonValue jsonValue : recArr) {
		    System.out.println(jsonValue);
		    populateWithJson(response, jsonValue);
		}
	}

	/**
	 * Initilize a JavaMongo talking to a specific hostPort, database, and collection.
	 * @param spec (JsonObject) speccifications 
	 *             - (String) hostPort: hostPort for mongod server, default is 27017
	 *             - (String) dbName: database name where to query the data from, default is Helios_Test
	 *             - (String) colName: collection name where to query the data from, default is Server_Test
	 */
	private void connectToJavaMongo(JsonObject spec) {
		String hostPort = spec.getString("hostPort", "localhost:27017");
		String dbName = spec.getString("dbName", "Helios_Test");
		String colName = spec.getString("colName", "Server_Test");
		javamongo = new Javamongo(hostPort, dbName, colName);
	}

	private void populateWithJson(HttpServletResponse response, JsonValue jval) {
	    if(jval != null) {
	        response.setContentType("text/x-json;charset=UTF-8");           
	        response.setHeader("Cache-Control", "no-cache");
	        try {
	             response.getWriter().write(jval.toString());
	        } catch (IOException ie) {
	        	System.out.println("Failed to send data to server response");
	        }                               
	    }
	}
}