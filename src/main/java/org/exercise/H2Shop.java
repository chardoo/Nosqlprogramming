package org.example;

import java.sql.*;
/**
 * Shop implementation using only H2 database for persistence
 */
public class H2Shop implements Shop {
    private Connection connection;
    
    public H2Shop(String dbPath) throws SQLException {
        // Connect to H2 database
        connection = DriverManager.getConnection("jdbc:h2:" + dbPath, "sa", "");
        initializeTables();
    }

    private void initializeTables() throws SQLException {
        Statement stmt = connection.createStatement();
        
        // Create products table
        stmt.execute("CREATE TABLE IF NOT EXISTS products (" +
                    "productId INT PRIMARY KEY, " +
                    "productName VARCHAR(255))");

        stmt.execute("CREATE TABLE IF NOT EXISTS purchases (" +
                    "customerId INT, " +
                    "productId INT, " +
                    "purchaseTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "PRIMARY KEY (customerId, productId))");
        
        stmt.close();
    }
    
    @Override
    public void addProductToShop(int productId, String productName) {
        try {
            PreparedStatement pstmt = connection.prepareStatement(
                "MERGE INTO products (productId, productName) VALUES (?, ?)");
            pstmt.setInt(1, productId);
            pstmt.setString(2, productName);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void buyProduct(int customerId, int productId) {
        try {
            PreparedStatement pstmt = connection.prepareStatement(
                "MERGE INTO purchases (customerId, productId) VALUES (?, ?)");
            pstmt.setInt(1, customerId);
            pstmt.setInt(2, productId);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public boolean hasBought(int customerId, int productId) {
        try {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT COUNT(*) FROM purchases WHERE customerId = ? AND productId = ?");
            pstmt.setInt(1, customerId);
            pstmt.setInt(2, productId);
            ResultSet rs = pstmt.executeQuery();
            
            if (rs.next()) {
                boolean result = rs.getInt(1) > 0;
                rs.close();
                pstmt.close();
                return result;
            }
            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
    
    @Override
    public String getProduct(int productId) {
        try {
            PreparedStatement pstmt = connection.prepareStatement(
                "SELECT productName FROM products WHERE productId = ?");
            pstmt.setInt(1, productId);
            ResultSet rs = pstmt.executeQuery();
            
            if (rs.next()) {
                String name = rs.getString("productName");
                rs.close();
                pstmt.close();
                return name;
            }
            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}