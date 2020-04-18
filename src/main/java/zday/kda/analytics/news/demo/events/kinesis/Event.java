package zday.kda.analytics.news.demo.events.kinesis;

import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Event {
  private static final String TYPE_FIELD = "Type";

  private static final Logger LOG = LoggerFactory.getLogger(Event.class);

  private static final Gson gson = new GsonBuilder().setPrettyPrinting()
          .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
          .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> Instant.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss zzz")
                  .parse(json.getAsString())))
          .create();

  public static Event parseEvent(byte[] event) {
    //parse the event payload and remove the type attribute
    JsonReader jsonReader =  new JsonReader(new InputStreamReader(new ByteArrayInputStream(event)));
    JsonElement jsonElement = Streams.parse(jsonReader);
    JsonElement labelJsonElement = jsonElement.getAsJsonObject().remove(TYPE_FIELD);

    if (labelJsonElement == null) {
      //return gson.fromJson(jsonElement, NewsEvent.class);
      throw new IllegalArgumentException("Event does not define a type field: " + new String(event));
    }

    //convert json to POJO, based on the type attribute
    switch (labelJsonElement.getAsString()) {
      case "watermark":
        return gson.fromJson(jsonElement, WatermarkEvent.class);
      case "news":
        return gson.fromJson(jsonElement, NewsEvent.class);
      default:
        throw new IllegalArgumentException("Found unsupported event type: " + labelJsonElement.getAsString());
    }
  }

  /**
   * @return timestamp in epoch millies
   */
  public abstract long getTimestamp();
}