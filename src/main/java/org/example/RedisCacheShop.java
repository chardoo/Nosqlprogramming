package org.example;

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.HostAndPort;
import java.sql.SQLException;
import java.util.List;

public class RedisCacheShop implements Shop {
    private final H2Shop h2Shop; // delegate for all DB logic
    private final UnifiedJedis jedis;
    private static final int MAX_RECENT_PURCHASES = 5;

    public RedisCacheShop(String dbPath, String redisHost, int redisPort) throws SQLException {
        // Use existing H2Shop for persistence
        this.h2Shop = new H2Shop(dbPath);
        System.out.println("Connecting to Redis at " + redisHost + ":" + redisPort);
        this.jedis = new UnifiedJedis(new HostAndPort(redisHost, redisPort));
        System.out.println("RedisCacheShop initialized successfully");
    }

    public RedisCacheShop(String dbPath) throws SQLException {
        this(dbPath, "localhost", 6379);
    }

    @Override
    public void addProductToShop(int productId, String productName) {
        // 1. Write to H2
        h2Shop.addProductToShop(productId, productName);
        // 2. Cache in Redis
        System.out.println("Caching product in Redis (productId: " + productId + ")");
        jedis.set("product:" + productId, productName);
    }

    @Override
    public void buyProduct(int customerId, int productId) {
        // 1. Write to H2
        h2Shop.buyProduct(customerId, productId);
        // 2. Add to Redis list of recent purchases
        String customerKey = "customer:" + customerId + ":purchases";
        jedis.lpush(customerKey, String.valueOf(productId));
        jedis.ltrim(customerKey, 0, MAX_RECENT_PURCHASES - 1);
    }

    @Override
    public boolean hasBought(int customerId, int productId) {
        String customerKey = "customer:" + customerId + ":purchases";
        List<String> recent = jedis.lrange(customerKey, 0, -1);


        if (recent.contains(String.valueOf(productId))) {
            System.out.println("Cache hit for hasBought (" + customerId + ", " + productId + ")");
            return true;
        }
        System.out.println("Cache miss for hasBought (" + customerId + ", " + productId + ")");
        return h2Shop.hasBought(customerId, productId);
    }

    @Override
    public String getProduct(int productId) {
        String key = "product:" + productId;
        String cached = jedis.get(key);

        if (cached != null) {
            System.out.println("Cache hit for product: " + productId);
            return cached;
        }
        System.out.println("Cache miss for product: " + productId);
        String name = h2Shop.getProduct(productId);

        if (name != null) {
            jedis.set(key, name);
        }
        return name;
    }

    @Override
    public void close() {
        try {
            h2Shop.close();
        } catch (Exception ignored) {}

        if (jedis != null) {
            jedis.close();
        }
    }
}
