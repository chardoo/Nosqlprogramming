package org.example;

import java.sql.SQLException;

/**
 * Simulation class to test the Shop implementations
 */
public class ShopSimulation {
    
    public static void main(String[] args) {
        try {
            // Initialize the RedisCacheShop with H2 database and Redis cache
            RedisCacheShop shop = new RedisCacheShop("./shopdb");
            
            System.out.println("=== Testing Task 1.1f: getProduct method ===\n");
            
            // Add some products
            shop.addProductToShop(1, "Laptop");
            shop.addProductToShop(2, "Mouse");
            shop.addProductToShop(3, "Keyboard");
            
            // Test (i): Get product that exists in Redis
            System.out.println("Test (i): Product existing in Redis");
            String product1 = shop.getProduct(1);
            System.out.println("Product 1: " + product1);
            System.out.println();
            
            // Test (ii): Get product that doesn't exist in Redis or H2
            System.out.println("Test (ii): Product not in Redis or H2");
            String product999 = shop.getProduct(999);
            System.out.println("Product 999: " + (product999 == null ? "Not found" : product999));
            System.out.println();
            
            System.out.println("\n=== Testing Task 1.1g: Complete simulation ===\n");
            
            // Create more products
            shop.addProductToShop(4, "Monitor");
            shop.addProductToShop(5, "Webcam");
            shop.addProductToShop(6, "Headset");
            shop.addProductToShop(7, "Microphone");
            shop.addProductToShop(8, "USB Cable");
            shop.addProductToShop(9, "HDMI Cable");
            shop.addProductToShop(10, "Power Adapter");
            
            // Create customers for  simulating purchases
            int customer1 = 101;
            int customer2 = 102;
            int customer3 = 103;
            
            System.out.println("Customer 101 buys 7 products:");
            shop.buyProduct(customer1, 1);
            shop.buyProduct(customer1, 2);
            shop.buyProduct(customer1, 3);
            shop.buyProduct(customer1, 4);
            shop.buyProduct(customer1, 5);
            shop.buyProduct(customer1, 6); // This will push product 1 out of cache
            shop.buyProduct(customer1, 7); // This will push product 2 out of cache
            System.out.println();
            
            System.out.println("Customer 102 buys 6 products:");
            shop.buyProduct(customer2, 2);
            shop.buyProduct(customer2, 3);
            shop.buyProduct(customer2, 4);
            shop.buyProduct(customer2, 5);
            shop.buyProduct(customer2, 6);
            shop.buyProduct(customer2, 8); // This will push product 2 out of cache
            System.out.println();
            
            System.out.println("Customer 103 buys 3 products:");
            shop.buyProduct(customer3, 1);
            shop.buyProduct(customer3, 9);
            shop.buyProduct(customer3, 10);
            System.out.println();
            
            System.out.println("=== Testing hasBought method ===\n");
            
            // Case 1: (Customer, Product) pair in the cache
            System.out.println("Case 1: Check if customer 101 bought product 7 (should be in cache):");
            boolean case1 = shop.hasBought(customer1, 7);
            System.out.println("Result: " + case1);
            System.out.println();
            
            // Case 2: (Customer, Product) pair in H2 but not in cache
            System.out.println("Case 2: Check if customer 101 bought product 1 (in H2, not in cache):");
            boolean case2 = shop.hasBought(customer1, 1);
            System.out.println("Result: " + case2);
            System.out.println();
            
            // Case 3: Customer did not purchase the product
            System.out.println("Case 3: Check if customer 103 bought product 8 (not purchased):");
            boolean case3 = shop.hasBought(customer3, 8);
            System.out.println("Result: " + case3);
            System.out.println();
            
            // Additional verification
            System.out.println("=== Additional Verification ===\n");
            System.out.println("Customer 102's purchases (should show cache hit for recent purchases):");
            shop.hasBought(customer2, 8); // Most recent, in cache
            shop.hasBought(customer2, 6); // In cache
            shop.hasBought(customer2, 2); // Oldest, not in cache anymore
            System.out.println();
            
            // Clean up
            shop.close();
            System.out.println("Simulation completed successfully!");
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}