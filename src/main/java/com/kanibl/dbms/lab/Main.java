package com.kanibl.dbms.lab;


public class Main {

    public static void main(String[] args) throws Exception {

        DatabaseService databaseService = new DatabaseService("jdbc:derby:/tmp/my-db\n;create=true");

        databaseService.createSchema();

        System.out.println("Calling GetuserAllAsMap 4 times");
        databaseService.getUserAllAsMap();
        databaseService.getUserAllAsMap();
        databaseService.getUserAllAsMap();
        databaseService.getUserAllAsMap();

        System.out.println("========\n");

        System.out.println("Calling getUserById(1) 4 times");
        databaseService.getUserById(1);
        databaseService.getUserById(1);
        databaseService.getUserById(1);
        databaseService.getUserById(1);
        System.out.println("Calling getUserById(2) 4 times");
        databaseService.getUserById(2);
        databaseService.getUserById(2);
        databaseService.getUserById(2);
        databaseService.getUserById(2);
        System.out.println("Calling getUserById(3) 4 times");
        databaseService.getUserById(3);
        databaseService.getUserById(3);
        databaseService.getUserById(3);
        databaseService.getUserById(3);

        databaseService.closeConnection();


    }

}
