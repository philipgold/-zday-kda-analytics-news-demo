package zday.kda.analytics.news.demo.events.kinesis;

import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;

public class NewsEvent extends Event {
    public final Instant dateTime;
    @SerializedName("URL")
    public final String url;
    public final String title;
    public final String sharingImage;
    public final String langCode;
    public final double docTone;
    public final String domainCountryCode;
    public final String location;
    public final double lat;
    public final double lon;
    public final String countryCode;
    public final String adm1Code;
    public final String adm2Code;
    public final int geoType;
    public final String contextualText;

    private static final Logger LOG = LoggerFactory.getLogger(NewsEvent.class);

    public NewsEvent(Instant dateTime, String url, String title, String sharingImage, String langCode, double docTone, String domainCountryCode, String location, double lat, double lon, String countryCode, String adm1Code, String adm2Code, int geoType, String contextualText) {
        this.dateTime = dateTime;
        this.url = url;
        this.title = title;
        this.sharingImage = sharingImage;
        this.langCode = langCode;
        this.docTone = docTone;
        this.domainCountryCode = domainCountryCode;
        this.location = location;
        this.lat = lat;
        this.lon = lon;
        this.countryCode = countryCode;
        this.adm1Code = adm1Code;
        this.adm2Code = adm2Code;
        this.geoType = geoType;
        this.contextualText = contextualText;
    }

    public NewsEvent() {
        this.dateTime = Instant.EPOCH;
        this.url = "";
        this.title = "";
        this.sharingImage = "";
        this.langCode = "";
        this.docTone = 0;
        this.domainCountryCode = "";
        this.location = "";
        this.lat = 0;
        this.lon = 0;
        this.countryCode = "";
        this.adm1Code = "";
        this.adm2Code = "";
        this.geoType = 0;
        this.contextualText = "";
    }

    @Override
    public long getTimestamp() {
        return dateTime.toEpochMilli();
    }

    @Override
    public String toString() {
        return "NewsEvent{" +
                "dateTime=" + dateTime +
                ", url='" + url + '\'' +
                ", title='" + title + '\'' +
                ", sharingImage='" + sharingImage + '\'' +
                ", langCode='" + langCode + '\'' +
                ", docTone=" + docTone +
                ", domainCountryCode='" + domainCountryCode + '\'' +
                ", location='" + location + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                ", countryCode='" + countryCode + '\'' +
                ", adm1Code='" + adm1Code + '\'' +
                ", adm2Code='" + adm2Code + '\'' +
                ", geoType=" + geoType +
                ", contextualText='" + contextualText + '\'' +
                '}';
    }
}
