package in.ds256.Assignment0;

import java.util.ArrayList;

import org.json.JSONArray;

import org.json.JSONObject;

public class Parser {

	JSONObject jObj;

	public Parser(String input) {
		jObj = createJsonObject(input);
	}

	public Parser(JSONObject input) {
		jObj = input;
	}

	public Parser() {

	}

	private JSONObject createJsonObject(String input) {
		try {
			JSONObject jObj = new JSONObject(input);
			return jObj;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	public void setInputJson(String json) {
		jObj = createJsonObject(json);

	}

	public String getTweet() {
		try {
			return jObj.getString("text");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	public String getUser() {
		try {
			if (jObj == null)
				return null;
			
			return jObj.getString("id_str");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}

	public boolean checkIfDelete() {
		try {
			Object a = null;

			if (jObj == null)
				return true;

			a = jObj.get("delete");
			if (a != null)
				return true;
			
		} catch (Exception e) {
			System.out.println("JSR" + e.getMessage());
			return false;
		}
		return false;
	}

	public int getHashTags() {
		ArrayList<String> hashtags = new ArrayList<>();
		try {
			if (jObj == null)
				return 0;
			
			JSONObject entities = jObj.getJSONObject("entities");
			JSONArray hashtagsArray = entities.getJSONArray("hashtags");
			
			for (int i = 0; i < hashtagsArray.length(); i++) {
				hashtags.add(hashtagsArray.getJSONObject(i).getString("text"));
			}
			
			return hashtags.size();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return 0;
		}
	}

	public ArrayList<String> getHashTagArray() {
		ArrayList<String> hashtags = new ArrayList<>();
		try {
			if (jObj == null)
				return hashtags;
			
			JSONObject entities = jObj.getJSONObject("entities");
			JSONArray hashtagsArray = entities.getJSONArray("hashtags");
			for (int i = 0; i < hashtagsArray.length(); i++) {
				hashtags.add(hashtagsArray.getJSONObject(i).getString("text"));
			}
			return hashtags;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return hashtags;
		}
	}
}
