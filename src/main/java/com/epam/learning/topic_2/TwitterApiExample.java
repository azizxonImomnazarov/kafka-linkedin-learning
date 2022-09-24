package com.epam.learning.topic_2;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.Get2TweetsSearchRecentResponse;
import com.twitter.clientlib.model.ResourceUnauthorizedProblem;
import com.twitter.clientlib.model.RulesLookupResponse;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Properties;
import java.util.Set;

public class TwitterApiExample {

    private static final String BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAAewhAEAAAAAltXMEPa%2BaIXTbIh5jMwNiQfrIfU%3DW7VKhgBewZQbPb2u3NXPAsOqS8Qg18za8UpeWQxrUsTrQG3Nqx";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";


    public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(BEARER_TOKEN));

        // search tweets
        String query = "#java"; // String | One query/rule/filter for matching Tweets. Refer to https://t.co/rulelength to identify the max query length.
        OffsetDateTime startTime = OffsetDateTime.now().minus(Duration.ofHours(1)); // OffsetDateTime | YYYY-MM-DDTHH:mm:ssZ. The oldest UTC timestamp from which the Tweets will be provided. Timestamp is in second granularity and is inclusive (i.e. 12:00:01 includes the first second of the minute).
        OffsetDateTime endTime = OffsetDateTime.now().minus(Duration.ofSeconds(20)); // OffsetDateTime | YYYY-MM-DDTHH:mm:ssZ. The newest, most recent UTC timestamp to which the Tweets will be provided. Timestamp is in second granularity and is exclusive (i.e. 12:00:01 excludes the first second of the minute).
        Integer maxResults = 20; // Integer | The maximum number of search results to be returned by a request.
        Set<String> tweetFields = Set.of("id", "text", "created_at", "author_id"); // Set<String> | A comma separated list of Tweet fields to display.

        try {
            // findTweetById
//            Get2TweetsSearchRecentResponse result = apiInstance.tweets().tweetsRecentSearch(query)
//                    .startTime(startTime)
//                    .endTime(endTime)
////                    .sinceId(sinceId)
////                    .untilId(untilId)
//                    .maxResults(maxResults)
//                    .tweetFields(tweetFields)
////                    .expansions(expansions)
////                    .mediaFields(mediaFields)
////                    .pollFields(pollFields)
////                    .userFields(userFields)
////                    .placeFields(placeFields)
//                    .execute();

            RulesLookupResponse result = apiInstance.tweets().getRules()
                    .maxResults(maxResults)
                    .execute();


//            if(result.getErrors() != null && result.getErrors().size() > 0) {
//                System.out.println("Error:");
//
//                result.getErrors().forEach(e -> {
//                    System.out.println(e.toString());
//                    if (e instanceof ResourceUnauthorizedProblem) {
//                        System.out.println(((ResourceUnauthorizedProblem) e).getTitle() + " " + ((ResourceUnauthorizedProblem) e).getDetail());
//                    }
//                });
//            } else {
            System.out.println(result);

//            }
        } catch (ApiException e) {
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}

