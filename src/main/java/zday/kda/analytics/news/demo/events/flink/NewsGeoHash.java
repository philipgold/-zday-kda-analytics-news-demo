package zday.kda.analytics.news.demo.events.flink;

public class NewsGeoHash {
    public final String geoHash;

    public NewsGeoHash() {
        this.geoHash = "";
    }

    public NewsGeoHash(String geoHash) {
        this.geoHash = geoHash;
    }
}
