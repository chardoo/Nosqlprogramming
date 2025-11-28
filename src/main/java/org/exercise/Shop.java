package org.example;

public interface Shop  {
    void addProductToShop(int productId, String productName);
    void buyProduct(int customerId, int productId);
    boolean hasBought(int customerId, int productId);

    String getProduct(int productId);
    void close();
}