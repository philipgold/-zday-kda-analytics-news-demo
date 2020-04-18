package zday.kda.analytics.news.demo.events.es;

public class NewsGeoCount extends Document {
    public final String location;
    public final long newsGeoCount;

    public NewsGeoCount(String location, long newsGeoCount, long timestamp) {
        super(timestamp);

        this.location = location;
        this.newsGeoCount = newsGeoCount;
    }
}
