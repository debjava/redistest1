package com.ddlab.rnd;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Test1 {

  public static void showRedisKeyExamples() {
    Jedis jedis = new Jedis("192.168.119.139", 6379);
    jedis.set("events/city/rome", "32,15,223,828");
    String cachedResponse = jedis.get("events/city/rome");
    System.out.println("cachedResponse = " + cachedResponse);
  }

  public static void showRedisListsExamples() {
    Jedis jedis = new Jedis("192.168.119.139", 6379);
    jedis.lpush("queue#tasks", "firstTask");
    jedis.lpush("queue#tasks", "secondTask");

    String task = jedis.rpop("queue#tasks");
    System.out.println("task = " + task);
    task = jedis.rpop("queue#tasks");
    System.out.println("task = " + task);
  }

  public static void showRedisSetsExamples() {
    Jedis jedis = new Jedis("192.168.119.139", 6379);
    jedis.sadd("nicknames", "nickname#1");
    jedis.sadd("nicknames", "nickname#2");
    jedis.sadd("nicknames", "nickname#1");

    Set<String> nicknames = jedis.smembers("nicknames");
    System.out.println("All Names : " + jedis.smembers("nicknames"));
    System.out.println("nicknames = " + nicknames);
    boolean exists = jedis.sismember("nicknames", "nickname#1");
    System.out.println("exists = " + exists);
  }

  public static void showRedisHashesExamples() {
    Jedis jedis = new Jedis("192.168.119.139", 6379);
    jedis.hset("user#1", "name", "Peter");
    jedis.hset("user#1", "job", "politician");
    String name = jedis.hget("user#1", "name");
    Map<String, String> fields = jedis.hgetAll("user#1");
    System.out.println("fields = " + fields);
    String job = fields.get("job");
    System.out.println("job = " + job);
  }

  public static void showRedisSortedSetExamples() {
    Jedis jedis = new Jedis("192.168.119.139", 6379);
    Map<String, Double> scores = new HashMap<>();

    scores.put("PlayerOne", 3000.0);
    scores.put("PlayerTwo", 1500.0);
    scores.put("PlayerThree", 8200.0);
    String key = "ranking";

    scores
        .entrySet()
        .forEach(
            playerScore -> {
              jedis.zadd(key, playerScore.getValue(), playerScore.getKey());
            });

    String player = jedis.zrevrange("ranking", 0, 1).iterator().next();
    System.out.println("player = " + player);
    long rank = jedis.zrevrank("ranking", "PlayerOne");
    System.out.println("rank = " + rank);
  }

  public static void showRedisTransactionExamples() {
    Jedis jedis = new Jedis("192.168.119.139", 6379);
    String friendsPrefix = "friends#";
    String userOneId = "4352523";
    String userTwoId = "5552321";

    Transaction t = jedis.multi();
    t.sadd(friendsPrefix + userOneId, userTwoId);
    t.sadd(friendsPrefix + userTwoId, userOneId);
    t.exec();

    Set<String> nicknames = jedis.smembers(friendsPrefix + userOneId);
    System.out.println("Txn : " + nicknames);
  }

  public static void showRedisPipelineExamples() {
    Jedis jedis = new Jedis("192.168.119.139", 6379);
    String userOneId = "4352523";
    String userTwoId = "4849888";

    Pipeline p = jedis.pipelined();
    p.sadd("searched#" + userOneId, "paris");
    p.zadd("ranking", 126, userOneId);
    p.zadd("ranking", 325, userTwoId);
    Response<Boolean> pipeExists = p.sismember("searched#" + userOneId, "paris");
    System.out.println("pipeExists = " + pipeExists);
    Response<Set<String>> pipeRanking = p.zrange("ranking", 0, -1);
    p.sync();

    boolean exists = pipeExists.get();
    Set<String> ranking = pipeRanking.get();
    System.out.println("ranking = " + ranking);
  }

  public static void main(String[] args) {
    showRedisKeyExamples();
    showRedisListsExamples();
    showRedisSetsExamples();
    showRedisHashesExamples();
    showRedisSortedSetExamples();
    showRedisTransactionExamples();
    showRedisPipelineExamples();
  }
}
