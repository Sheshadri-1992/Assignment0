package in.ds256.Assignment0;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Parser {

	JSONObject jObj;

	public Parser(String input) {
		jObj = createJsonObject(input);
	}
	
	public Parser(JSONObject input) {
		jObj = input;
	}

	private JSONObject createJsonObject(String input) {
		try {
			JSONObject jObj = new JSONObject(input);
			return jObj;
		} catch (JSONException e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	public String getTweet() {
		try {
			return jObj.getString("text");
		} catch (JSONException e) {
			System.out.println(e.getMessage());
			return null;
		}
	}
	
	public String getUser() {
		try {
			return jObj.getString("id_str");
		} catch (JSONException e) {
			System.out.println(e.getMessage());
			return null;
		}
	}
	
	public boolean checkIfDelete() {
		try {
			Object a = null;
			a = jObj.get("delete");
			if(a!=null)
				return true;
		} catch (JSONException e) {
//			System.out.println("JSR"+e.getMessage());
			return false;
		}
		return false;
	}

	public ArrayList<String> getHashTags() {
		ArrayList<String> hashtags = new ArrayList<>();
		try {
		JSONObject entities = jObj.getJSONObject("entities");
		JSONArray hashtagsArray = entities.getJSONArray("hashtags");
		for(int i=0;i<hashtagsArray.length();i++) {
			hashtags.add(hashtagsArray.getJSONObject(i).getString("text"));
		}
		return hashtags;
		}
		catch(JSONException e)
		{
			System.out.println(e.getMessage());
			return null;
		}
	}
}
