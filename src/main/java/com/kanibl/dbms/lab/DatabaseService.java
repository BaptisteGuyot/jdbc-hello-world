package com.kanibl.dbms.lab;

import com.google.gson.GsonBuilder;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.*;
import java.util.*;

import com.google.gson.Gson;
import redis.clients.jedis.params.SetParams;

public class DatabaseService {

    private String jdbcString;
    private Connection connection;
    private JedisPool pool;

    public DatabaseService(String jdbcString) throws Exception {
        pool = new JedisPool(new JedisPoolConfig(), "redis-12402.c9.us-east-1-2.ec2.cloud.redislabs.com", 12402,200, "nantes");
        this.jdbcString = jdbcString;
        connectoToDB();
    }

    public void connectoToDB() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (connection == null) {
            connection = DriverManager.getConnection(jdbcString);
        }
    }

    public void createSchema() throws SQLException {
        Statement stmt = connection.createStatement();
        try {
            stmt.executeUpdate("DROP TABLE users");
        } catch (Exception e) {
            System.out.println("Drop table failed - probably because it does not exist");
        }
        stmt.executeUpdate("CREATE TABLE users (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50))");
        System.out.println("Table user created");
        stmt.executeUpdate("INSERT INTO users VALUES (1,'John', 'Doe')");
        stmt.executeUpdate("INSERT INTO users VALUES (2,'David', 'Getta')");
        stmt.executeUpdate("INSERT INTO users VALUES (3,'Billie', 'Eilish')");
        System.out.println("3 User records inserted");
        stmt.close();
    }

    public List<Map<String, String>>  getUserAllAsMap() throws SQLException {
        List<Map<String,String>> data = null;
        long start = System.currentTimeMillis();
        Gson gson = new Gson();
        String sentence = "";
        try(Jedis jedis = pool.getResource()){
            data = gson.fromJson(jedis.get("bguyotGetUserAllAsMap"), List.class);
            if(data == null){
        sentence = " - No cache";
        String query = "SELECT * FROM users";

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
        data = new ArrayList<Map<String,String>>();

        for(int i = 1; i <= rsmd.getColumnCount(); i++){
            columns.add(rsmd.getColumnName(i));
        }
        while(rs.next()){
            data.add(getRsAsMap(columns,  rs));
        }
        rs.close();
        stmt.close();
        jedis.set("bguyotGetUserAllAsMap",gson.toJson(data), SetParams.setParams().ex(120));
            }else{
                sentence=" - From cache";
            }
        }catch(Exception e){

        }
        long end = System.currentTimeMillis();
        System.out.println("Call GetUserAllAsMap : "+ (end-start) +"ms"+sentence );
        return data;
    }

    public Map<String, String> getUserById(int id) throws SQLException {
        Map<String,String> data = null;
        long start = System.currentTimeMillis();
        Gson gson = new Gson();
        String sentence = "";
        try(Jedis jedis = pool.getResource()){
            data = gson.fromJson(jedis.get("bguyotGetUserById"+id), Map.class);
            if(data == null){
        sentence = " - No cache";

        String query = "SELECT * FROM users WHERE id = " + id;

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
        data = new HashMap<String, String>();

        for(int i = 1; i <= rsmd.getColumnCount(); i++){
            columns.add(rsmd.getColumnName(i));
        }
        while(rs.next()){
            data = getRsAsMap(columns,  rs);
        }
        rs.close();
        stmt.close();
        jedis.set("bguyotGetUserById"+id,gson.toJson(data), SetParams.setParams().ex(120));
            }else{
                sentence = " - From cache";
            }
        }catch(Exception e){

        }
        long end = System.currentTimeMillis();
        System.out.println("Call getUserById : "+ (end-start) +"ms" +sentence);
        return data;
    }

    private Map<String,String> getRsAsMap(List<String> columns, ResultSet rs) throws SQLException {
        Map<String,String> row = new HashMap<String, String>(columns.size());
        for(String col : columns) {
            row.put(col, rs.getString(col));
        }
        return row;
    }

    public void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        try (Jedis jedis = pool.getResource()) {
            Set<String> keys = jedis.keys("bguyot*");
            for(String key : keys) {
                jedis.del(key);
            }
        }
    }
}
