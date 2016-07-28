package com.dataartisans.domain;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * This POJO represents an impression on a given tweet.
 */
public class TweetImpression {

  private Long tweetId;

  private String date;

  public TweetImpression(){
    this(-1L);
  }

  public TweetImpression(long tweetId) {
    this.tweetId = tweetId;
  }

  public void setTweetId(long tweetId){
    this.tweetId = tweetId;
  }

  public Long getTweetId(){
    return tweetId;
  }

  public static KeySelector<TweetImpression, Long> getKeySelector() {
    return new KeySelector<TweetImpression, Long>() {
      @Override
      public Long getKey(TweetImpression tweet) throws Exception {
        return tweet.tweetId;
      }
    };
  }

  @Override
  public String toString() {
    return String.format("TweetImpression(%d)", tweetId);
  }
}
