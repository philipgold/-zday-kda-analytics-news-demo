package zday.kda.analytics.news.demo;

import com.amazonaws.regions.Regions;
import zday.kda.analytics.news.demo.events.EventDeserializationSchema;
import zday.kda.analytics.news.demo.events.es.NewsGeoCount;
import zday.kda.analytics.news.demo.events.es.NewsWordCount;
import zday.kda.analytics.news.demo.events.kinesis.Event;
import zday.kda.analytics.news.demo.events.kinesis.NewsEvent;
import zday.kda.analytics.news.demo.operators.NewsCountByGeoHash;
import zday.kda.analytics.news.demo.operators.NewsCountByWords;
import zday.kda.analytics.news.demo.operators.NewsToGeoHash;
import zday.kda.analytics.news.demo.utils.ParameterToolUtils;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zday.kda.analytics.news.demo.operators.AmazonElasticsearchSink;


public class ProcessNewsStream {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessNewsStream.class);

    private static final String DEFAULT_STREAM_NAME = "streaming-analytics-demo";
    private static final String DEFAULT_REGION_NAME = Regions.getCurrentRegion() == null ? "eu-west-1" : Regions.getCurrentRegion().getName();


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameter;

        if (env instanceof LocalStreamEnvironment) {
            //read the parameters specified from the command line
            parameter = ParameterTool.fromArgs(args);
        } else {
            //read the parameters from the Kinesis Analytics environment
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();

            Properties flinkProperties = applicationProperties.get("FlinkApplicationProperties");

            if (flinkProperties == null) {
                throw new RuntimeException("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime.");
            }

            parameter = ParameterToolUtils.fromApplicationProperties(flinkProperties);
        }


        //enable event time processing
        if (parameter.get("EventTime", "false").equals("true")) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }


        //set Kinesis consumer properties
        Properties kinesisConsumerConfig = new Properties();
        //set the region the Kinesis stream is located in
        kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION, parameter.get("Region", DEFAULT_REGION_NAME));
        //obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
        kinesisConsumerConfig.setProperty(
                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO"
        );
        //poll new events from the Kinesis stream once every second
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "1000");


        //create Kinesis source
        DataStream<Event> kinesisStream = env.addSource(new FlinkKinesisConsumer<>(
                //read events from the Kinesis stream passed in as a parameter
                parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
                //deserialize events with EventSchema
                new EventDeserializationSchema(),
                //using the previously defined properties
                kinesisConsumerConfig
        ));

        DataStream<NewsEvent> news = kinesisStream
                //extract watermarks from watermark events
                //.assignTimestampsAndWatermarks(new TimestampAssigner())
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


        if (parameter.has("ElasticsearchEndpoint")) {
            String elasticsearchEndpoint = parameter.get("ElasticsearchEndpoint");
            final String region = parameter.get("Region", DEFAULT_REGION_NAME);

            //remove trailling /
            if (elasticsearchEndpoint.endsWith(("/"))) {
                elasticsearchEndpoint = elasticsearchEndpoint.substring(0, elasticsearchEndpoint.length() - 1);
            }

            newsGeoCount.addSink(AmazonElasticsearchSink
                .buildElasticsearchSink(elasticsearchEndpoint, region, "news_position", "news_position"));
            newsWordCounts.addSink(AmazonElasticsearchSink.buildElasticsearchSink(elasticsearchEndpoint, region, "news_words", "news_words"));
        }


        LOG.info("Reading events from stream {}", parameter.get("InputStreamName", DEFAULT_STREAM_NAME));

        env.execute();
    }
}