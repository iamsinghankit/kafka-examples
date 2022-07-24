package org.github.kafka.examples.stream.click;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.github.kafka.examples.stream.Constants;
import org.github.kafka.examples.stream.JsonDeserializer;
import org.github.kafka.examples.stream.JsonSerializer;

import java.time.Duration;
import java.util.Properties;

public class ClickStreamEnrichment implements Runnable{
    @Override
    public void run() {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clicks");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, UserActivitySerde.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, PageView> views = builder.stream(Constants.PAGE_VIEW_TOPIC, Consumed.with(Serdes.Integer(), new PageViewSerde()));
        KStream<Integer, Search> searches = builder.stream(Constants.SEARCH_TOPIC, Consumed.with( Serdes.Integer(), new SearchSerde()));
        KTable<Integer, UserProfile> profiles = builder.table(Constants.USER_PROFILE_TOPIC, Consumed.with(Serdes.Integer(), new ProfileSerde()));

        KStream<Integer, UserActivity> viewsWithProfile = views.leftJoin(profiles,
                    (page, profile) -> {
                        if (profile != null)
                            return new UserActivity(profile.getUserID(), profile.getUserName(), profile.getZipcode(), profile.getInterests(), "", page.page());
                        else
                           return new UserActivity(-1, "", "", null, "", page.page());

        });

        KStream<Integer, UserActivity> userActivityKStream = viewsWithProfile.leftJoin(searches,
                (userActivity, search) -> {
                    if (search != null)
                        userActivity.updateSearch(search.searchTerms());
                    else
                        userActivity.updateSearch("");
                    return userActivity;
                },
                JoinWindows.of(Duration.ofSeconds(1)),
                StreamJoined.with(Serdes.Integer(), new UserActivitySerde(), new SearchSerde()));

        userActivityKStream.to(Constants.USER_ACTIVITY_TOPIC, Produced.with(Serdes.Integer(), new UserActivitySerde()));

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.cleanUp();

        streams.start();


        sleepQuietly(Duration.ofSeconds(60));

        streams.close();


    }

    private void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ignored) {
        }
    }


    static public final class PageViewSerde extends Serdes.WrapperSerde<PageView> {
        public PageViewSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(PageView.class));
        }
    }

    static public final class ProfileSerde extends Serdes.WrapperSerde<UserProfile> {
        public ProfileSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(UserProfile.class));
        }
    }

    static public final class SearchSerde extends Serdes.WrapperSerde<Search> {
        public SearchSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Search.class));
        }
    }

    static public final class UserActivitySerde extends Serdes.WrapperSerde<UserActivity> {
        public UserActivitySerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(UserActivity.class));
        }
    }
}