package com.epam.learning.topic_2;

// Import classes:

import com.google.common.reflect.TypeToken;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.JSON;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.FilteredStreamingTweetResponse;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Example {

    private static final String BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAAewhAEAAAAAltXMEPa%2BaIXTbIh5jMwNiQfrIfU%3DW7VKhgBewZQbPb2u3NXPAsOqS8Qg18za8UpeWQxrUsTrQG3Nqx";

    public static void main(String[] args) {
        // Set the credentials based on the API's "security" tag values.
        // Check the API definition in https://api.twitter.com/2/openapi.json
        // When multiple options exist, the SDK supports only "OAuth2UserToken" or "BearerToken"

        // Uncomment and set the credentials configuration

        // Configure HTTP bearer authorization:
        // TwitterCredentialsBearer credentials = new TwitterCredentialsBearer(System.getenv("TWITTER_BEARER_TOKEN"));
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(BEARER_TOKEN));

        // Set the params values
        Integer backfillMinutes = 56; // Integer | The number of minutes of backfill requested.
        Set<String> tweetFields = Set.of("id", "text", "created_at", "author_id"); // Set<String> | A comma separated list of Tweet fields to display.
        Set<String> expansions = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of fields to expand.
        Set<String> mediaFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of Media fields to display.
        Set<String> pollFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of Poll fields to display.
        Set<String> userFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of User fields to display.
        Set<String> placeFields = new HashSet<>(Arrays.asList()); // Set<String> | A comma separated list of Place fields to display.
        try {
            InputStream result = apiInstance.tweets().sampleStream()
//                    .backfillMinutes(backfillMinutes)
                    .tweetFields(tweetFields)
//                    .expansions(expansions)
//                    .mediaFields(mediaFields)
//                    .pollFields(pollFields)
//                    .userFields(userFields)
//                    .placeFields(placeFields)
                    .execute();
            try{
                JSON json = new JSON();
                Type localVarReturnType = new TypeToken<FilteredStreamingTweetResponse>(){}.getType();
                BufferedReader reader = new BufferedReader(new InputStreamReader(result));
                String line = reader.readLine();
                while (line != null) {
                    if(line.isEmpty()) {
                        System.out.println("==> Empty line");
                        line = reader.readLine();
                        continue;
                    }
                    Object jsonObject = json.getGson().fromJson(line, localVarReturnType);
                    System.out.println(jsonObject != null ? jsonObject.toString() : "Null object");
                    line = reader.readLine();
                }
            }catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }
        } catch (ApiException e) {
            System.err.println("Exception when calling TweetsApi#searchStream");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}