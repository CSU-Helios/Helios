package helios.javadriver;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonString;

public class Javamongo{
	private final static String databaseName = "Helios";

	private final static String collectionName = "TrafficData";

	private final static String hostPort = "localhost:27017";
	
	private MongoCollection<Document> collection;

	private MongoClient mongoClient;
	
	public Javamongo(){
		this(hostPort, databaseName, collectionName);	
	}
	
	/**
	 * Initialization, establish connection to MongoDB and open a specific collection
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
	 * query the database based on the specifications in Json format
	 * 
	 * @param  spec (JsonObject) specifications 
	 * 			- (String) geohash: central geohash for querying, should less than 12 characters
	 * 			- (String) timestamp: restrict data starting from a timestamp to present (TODO, from --> to)
	 * 				+ 'from': A string of UTC time in milliseconds
	 * 				+ 'to': A string of UTC time in milliseconds, could be None, if None, set to current time
	 * 			- (String) zoomLevel: 
	 * 				+ 'heatPoints': retrieve number of accidents happened in a specific geohash area
	 * 				+ 'allData': retrieve detailed information of accidents happened in a specific geohash area
	 * 			- (String) numLimit: total number of records return to client for a single query, default max int
	 * 			- TODO: (String) type: historical or prediction
	 * @return     JsonArray with all records in JsonObject format
	 */
	public JsonArray queryCollectionRetJson(JsonObject spec){
		Bson filterBson = generateFilterDoc(spec);
		int numLimit = Integer.valueOf(spec.getString("numLimit", "2147483647"));
		String zoomLevel = spec.getString("zoomLevel", "allData");
		MongoCursor<Document> cursor = null;
		switch (zoomLevel){
			case "heatPoints":
				Bson fields = Projections.include("severity", "point.geohash", "toPoint.geohash");
				cursor = collection.find(filterBson).limit(numLimit).projection(fields).iterator();
				break;
			case "allData":
				cursor = collection.find(filterBson).limit(numLimit).iterator();
				break;
			case "number":
				// TODO Need improvment
				// Cursor.count() does not work now
				cursor = collection.find(filterBson).limit(numLimit).iterator();
				int count  = 0;
				while(cursor.hasNext()) {
					count ++;
					cursor.next();
				}
				JsonArrayBuilder builder = Json.createArrayBuilder()
											.add(Json.createObjectBuilder()
													.add("count", count).build());
				return builder.build();
		}

		JsonArrayBuilder builder = Json.createArrayBuilder();
		while(cursor.hasNext()) {
		    JsonReader jsonReader = Json.createReader(new StringReader((cursor.next().toJson())));
			JsonObject jsonObj = jsonReader.readObject();
			jsonReader.close();
			builder.add(jsonObj);
		}
		return builder.build();
	}
	
	/**
	 * @param  spec (JsonObject) specifications 
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
			JsonArray geohashes = spec.getJsonArray("geohash");
			List<Bson> bsonGeoFilters = new ArrayList<Bson>();
			for (JsonValue geohashValue : geohashes) {
				String geohash = ((JsonString) geohashValue).getString();
				bsonGeoFilters.add(Filters.or(Filters.regex("point.geohash", geohash + ".*"), 
					 						Filters.regex("toPoint.geohash", geohash + ".*")));
			}
			Bson geohashesFilter = Filters.or(bsonGeoFilters);
			
			String timestampFrom = spec.getJsonObject("timestamp").getString("from");
			String timestampTo = spec.getJsonObject("timestamp").getString("to", String.valueOf(System.currentTimeMillis()));
			Bson timestampFilter = Filters.and(Filters.gte("start", "/Date(" + timestampFrom + ")/"),
											 Filters.lte("start", "/Date(" + timestampTo + ")/"));
			filterBson = Filters.and(geohashesFilter, timestampFilter);
		} catch (NullPointerException ne){
			System.out.println("Illegal Query Speccifications");
		}
		System.out.println(filterBson.toString());
		return filterBson;
	}

	public static void main(String[] args){
		Javamongo javamongo = new Javamongo();
		
		JsonObject jobj = Json.createObjectBuilder()
							.add("geohash", Json.createArrayBuilder()
												.add("9xh1")
												.add("9xh2"))
							.add("timestamp", Json.createObjectBuilder()
												.add("from", "1522118500000")
								)
							.add("zoomLevel", "heatPoints")
							.add("numLimit", "100").build();
		System.out.println(jobj);
		JsonArray jarr = javamongo.queryCollectionRetJson(jobj);
		System.out.println(jarr.size());
		for(JsonValue value : jarr){
			System.out.println(value.toString());
		}
	}	
}
