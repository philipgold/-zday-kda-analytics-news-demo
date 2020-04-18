package zday.kda.analytics.news.demo.events.es;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;

import java.time.Instant;
import java.time.format.DateTimeFormatter;


public abstract class Document {
  private static final Gson gson = new GsonBuilder()
      //.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES
          //.setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
      .create();

  public final long timestamp;

  public Document(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return gson.toJson(this);
  }
}
