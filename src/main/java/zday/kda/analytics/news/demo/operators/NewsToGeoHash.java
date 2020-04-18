package zday.kda.analytics.news.demo.operators;

import ch.hsr.geohash.GeoHash;
import zday.kda.analytics.news.demo.events.flink.NewsGeoHash;
import zday.kda.analytics.news.demo.events.kinesis.NewsEvent;
import org.apache.flink.api.common.functions.MapFunction;

public class NewsToGeoHash implements MapFunction<NewsEvent, NewsGeoHash> {
    @Override
    public NewsGeoHash map(NewsEvent newsEvent) {
        return new NewsGeoHash(GeoHash.geoHashStringWithCharacterPrecision(newsEvent.lat, newsEvent.lon, 7));
    }
}
