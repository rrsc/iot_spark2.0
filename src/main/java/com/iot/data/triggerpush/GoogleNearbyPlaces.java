package com.iot.data.triggerpush;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class GoogleNearbyPlaces {

	private static String googlePlacesAPI;
	private static String latitude, longitude;
	private static String radius;
	private static String advertiser;
	private static String apiKey;
	private static String url;
	
	public GoogleNearbyPlaces() {
		googlePlacesAPI = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?";
		latitude = "-33.8670522";
		longitude = "151.1957362";
		//in metres
		radius = "1000";
		advertiser = "Starbucks";
		apiKey = "AIzaSyBqiX4MzJ_xBc2SaVRNqVT-R6Cxg75Db8E";
		
		url = googlePlacesAPI + "location=" + latitude + "," + longitude + "&radius=" + radius + "&name=" + advertiser + "&key=" + apiKey;
		
	}
	
	private String callGooglePlacesAPI(String myURL) {
		System.out.println("Requeted URL:" + myURL);
		StringBuilder sb = new StringBuilder();
		URLConnection urlConn = null;
		InputStreamReader in = null;
		try {
			URL url = new URL(myURL);
			urlConn = url.openConnection();
			if (urlConn != null)
				urlConn.setReadTimeout(60 * 1000);
			if (urlConn != null && urlConn.getInputStream() != null) {
				in = new InputStreamReader(urlConn.getInputStream(), Charset.defaultCharset());
				BufferedReader bufferedReader = new BufferedReader(in);
				if (bufferedReader != null) {
					int cp;
					while ((cp = bufferedReader.read()) != -1) {
						sb.append((char) cp);
					}
					bufferedReader.close();
				}
			}
			in.close();
		} catch (Exception e) {
			throw new RuntimeException("Exception while calling URL:"+ myURL, e);
		}
		
		return sb.toString();
	}
	
	private StringBuilder parseGoogleResponse(String response) throws ParseException {
		System.out.println("*************************************");
		StringBuilder sb = new StringBuilder("[");
		
		JSONParser parser = new JSONParser();
		Object obj = parser.parse(response);
		JSONObject conf = (JSONObject) obj;
		
		obj = parser.parse(conf.get("results").toString());
		JSONArray conf2 = (JSONArray) obj;
		
		for(int i=0; i<conf2.size(); i++) {
			obj = parser.parse(conf2.get(i).toString());
			JSONObject conf3 = (JSONObject) obj;
			
			obj = parser.parse(conf3.get("geometry").toString());
			JSONObject conf4 = (JSONObject) obj;
			System.out.println(conf4.get("location"));
			sb.append(conf4.get("location") + ",");
			
		}
		
		sb = new StringBuilder(sb.substring(0, sb.length() - 1).trim().toString() + "]");
		return sb;
	}
	
	public static void main(String[] args) throws ParseException {
		
		GoogleNearbyPlaces googlePlaces = new GoogleNearbyPlaces();
		String jsonResults = googlePlaces.callGooglePlacesAPI(url);
		//System.out.println(jsonResults);
		StringBuilder sb = googlePlaces.parseGoogleResponse(jsonResults);
		System.out.println(sb.toString());
		
		
	}
}	