package helios.javadriver;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

public class Javamongo{
	private final static String databaseName = "Helios_Test";

//	private final static String collectionName = "Helios_Traffic_Data";
	private final static String collectionName = "Server_Test";

	private final static String hostPort = "localhost:27017";
	
	private MongoCollection<Document> collection;

	private MongoClient mongoClient;
	
	public Javamongo(){
		this(hostPort, databaseName, collectionName);	
	}
	
	/**
	 * Initialization, establish connection to MongoDB and open a speccific collection
	 * @param  hostPort host name and port number with format host:port
	 * @param  dbName   database name in MongoDB
	 * @param  colName  collection name in MongoDB database
	 * @return
	 */
	public Javamongo(String hostPort, String dbName, String colName){
		MongoClientURI connectionString = new MongoClientURI("mongodb://" + hostPort);
		mongoClient = new MongoClient(connectionString);
		MongoDatabase database = mongoClient.getDatabase(dbName);
		collection = database.getCollection(colName);
	}

	/**
	 * THE ONLY OUTGOING METHOD
	 * query the database based on the speccifications in Json format
	 * 
	 * @param  spec (JsonObject) speccifications 
	 * 			- (String) geohash: central geohash for querying, should less than 12 characters
	 * 			- (String) timestamp: retrict data starting from a timestamp to present (TODO, from --> to)
	 * 				+ 'from': A string of UTC time in milliseconds
	 * 				+ 'to': A string of UTC time in milliseconds, could be None, if None, set to current time
	 * 			- (String) zoomLevel: 
	 * 				+ 'heatPoints': retrieve number of acidents happened in a specific geohash area
	 * 				+ 'allData': retrieve detailed information of acidents happended in a specific geohash area
	 * 			- (String) numLimit: total number of records return to client for a single query, default max int
	 * @return     JsonArray with all records in JsonObject format
	 */
	public JsonArray queryCollectionRetJson(JsonObject spec){
		JsonArrayBuilder builder = Json.createArrayBuilder();
		Bson filterBson = generateFilterDoc(spec);
		int numLimit = Integer.valueOf(spec.getString("numLimit", "2147483647"));
		String zoomLevel = spec.getString("zoomLevel", "allData");
		FindIterable<Document> documents = collection.find(filterBson).limit(numLimit);
		for (Document doc : documents){
			JsonReader jsonReader = null;
			switch (zoomLevel){
				case "heatPoints":
					String json = Json.createObjectBuilder()
							.add("severity", doc.getInteger("severity"))
							.build().toString();
					jsonReader = Json.createReader(new StringReader(json));
					break;
				case "allData":
					jsonReader = Json.createReader(new StringReader(doc.toJson()));
					break;
			}
			JsonObject jsonObj = jsonReader.readObject();
			jsonReader.close();
			builder.add(jsonObj);
		}
		return builder.build();
	}
	
	/**
	 * @param  spec (JsonObject) speccifications 
	 * 			- (String) geohash: ex. 9xh2
	 * 				+ central geohash for querying, should less than 12 characters
	 * 			- (String) timestamp: 
	 * 				+ 'from': A string of UTC time in milliseconds ex. 1522183300000
	 * 				+ 'to': A string of UTC time in milliseconds, could be None, if None, set to current time
	 * @return Bson filterBson
	 */
	private Bson generateFilterDoc(JsonObject spec){
		Bson filterBson = new Document();
		try {
			String geohash = spec.getString("geohash");
			String timestampFrom = spec.getJsonObject("timestamp").getString("from");
			String timestampTo = spec.getJsonObject("timestamp").getString("to", String.valueOf(System.currentTimeMillis()));
			Bson geohashFilter = Filters.or(Filters.regex("point.geohash", geohash + ".*"), 
					 						Filters.regex("toPoint.geohash", geohash + ".*"));
			Bson timestampFilter = Filters.and(Filters.gte("start", "/Date(" + timestampFrom + ")/"),
											 Filters.lte("start", "/Date(" + timestampTo + ")/"));
			filterBson = Filters.and(geohashFilter, timestampFilter);
		} catch (NullPointerException ne){
			System.out.println("Illegal Query Speccifications");
		}
		return filterBson;
	}

	public static void main(String[] args){
		Javamongo javamongo = new Javamongo();
		
		JsonObject jobj = Json.createObjectBuilder()
							.add("geohash", "9x")
							.add("timestamp", Json.createObjectBuilder()
												.add("from", "1522118500000")
								)
							.add("zoomLevel", "heatPoints")
							.add("numLimit", "10").build();
		JsonArray jarr = javamongo.queryCollectionRetJson(jobj);
		
		for(JsonValue value : jarr){
			System.out.println(value.toString());
		}
	}	
}