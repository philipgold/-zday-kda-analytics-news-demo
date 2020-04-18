package zday.kda.analytics.news.demo;

import com.amazonaws.regions.Regions;
import zday.kda.analytics.news.demo.events.EventDeserializationSchema;
import zday.kda.analytics.news.demo.events.es.NewsGeoCount;
import zday.kda.analytics.news.demo.events.es.NewsWordCount;
import zday.kda.analytics.news.demo.events.kinesis.Event;
import zday.kda.analytics.news.demo.events.kinesis.NewsEvent;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zday.kda.analytics.news.demo.operators.AmazonElasticsearchSink;
import zday.kda.analytics.news.demo.operators.NewsCountByGeoHash;
import zday.kda.analytics.news.demo.operators.NewsCountByWords;
import zday.kda.analytics.news.demo.operators.NewsToGeoHash;


public class ProcessNewsStreamLocal {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessNewsStreamLocal.class);

    private static final String DEFAULT_STREAM_NAME = "flink-demo";
    private static final String DEFAULT_REGION_NAME = Regions.getCurrentRegion() == null ? "eu-west-1" : Regions.getCurrentRegion().getName();


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //read the parameters specified from the command line
        ParameterTool parameter = ParameterTool.fromArgs(args);


        Properties kinesisConsumerConfig = new Properties();
        //set the region the Kinesis stream is located in
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, parameter.get("Region", DEFAULT_REGION_NAME));
        //obtain credentials through the DefaultCredentialsProviderChain, which includes credentials from the instance metadata
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");
        //poll new events from the Kinesis stream once every second
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");


        //create Kinesis source
        DataStream<Event> kinesisStream = env.addSource(new FlinkKinesisConsumer<>(
                //read events from the Kinesis stream passed in as a parameter
                parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
                //deserialize events with EventSchema
                new EventDeserializationSchema(),
                //using the previously defined Kinesis consumer properties
                kinesisConsumerConfig
        ));

        DataStream<NewsEvent> news = kinesisStream
                //remove all events that aren't NewsEvents
                .filter(event -> NewsEvent.class.isAssignableFrom(event.getClass()))
                //cast Event to NewsEvent
                .map(event -> (NewsEvent) event);

        DataStream<NewsGeoCount> newsGeoCount = news
                //compute geo hash for every event
                .map(new NewsToGeoHash())
                .keyBy("geoHash")
                //collect all events in a one hour window
                .timeWindow(Time.minutes(1))
                //count events per geo hash in the one hour window
                .apply(new NewsCountByGeoHash());

        DataStream<NewsWordCount> newsWordCounts = news
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy("title")
                .timeWindow(Time.minutes(1))
                // split up the lines in pairs (2-tuples) containing: (title,1)
                .apply(new NewsCountByWords());

        final String region = parameter.get("Region", DEFAULT_REGION_NAME);
        String elasticsearchEndpoint1 = "https://search-kda-demo2-ekmzmkvjflvxv5r4iilcgjm44y.eu-west-1.es.amazonaws.com";
        newsGeoCount.addSink(AmazonElasticsearchSink
            .buildElasticsearchSink(elasticsearchEndpoint1, region, "news_position", "news_position"));
        newsWordCounts.addSink(AmazonElasticsearchSink.buildElasticsearchSink(elasticsearchEndpoint1, region, "news_words", "news_words"));

        LOG.info("Reading events from stream {}", parameter.get("InputStreamName", DEFAULT_STREAM_NAME));

        env.execute();
    }
}
