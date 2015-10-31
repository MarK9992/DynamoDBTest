package fr.polytech.unice.sacc;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.util.Tables;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Hello world!
 */
public class App {
    private static long cpt = 0;
    private static String tableName = "DynamoTP";
    private static String primaryKey = "productID";

    public static void add10000Items(DynamoDB dynamoDB) {
        long start = System.currentTimeMillis(), end;
        TableWriteItems tableWriteItems;

        for (int j = 0; j < 400; j++) {
            tableWriteItems = new TableWriteItems(tableName);
            for (int i = 0; i < 25; i++) {
                tableWriteItems.addItemToPut(new Item().withPrimaryKey(primaryKey, cpt++));
            }
            dynamoDB.batchWriteItem(tableWriteItems);
        }
        end = System.currentTimeMillis();
        System.out.println("time elapsed: " + (end - start));
    }

    public static void main(String[] args) {
        ProfilesConfigFile configFile = new ProfilesConfigFile(new File("src/main/resources/credentials.txt"));
        AmazonDynamoDBClient client = new AmazonDynamoDBClient(new ProfileCredentialsProvider(configFile, "SACC-dynamo"));
        client.setEndpoint("http://localhost:8000");
        client.setRegion(Region.getRegion(Regions.US_EAST_1));

        DynamoDB dynamoDB = new DynamoDB(client);

        if (Tables.doesTableExist(client, tableName)) {
            Table table = dynamoDB.getTable(tableName);

            // flushing

            //dynamoDB.

            // putting items

            long bookId = cpt++, dvdId = cpt++;
            Item book = new Item().withPrimaryKey(primaryKey, bookId).with("title", "Hello world!")
                    .with("plublication year", 2015).with("number of pages", 0);
            Item dvd = new Item().withPrimaryKey(primaryKey, dvdId).with("type", "DVD").with("year", 1909)
                    .with("duration", 1);

            table.putItem(book);
            table.putItem(dvd);

            // Queries

            HashMap<String, String> nameMap = new HashMap<String, String>();
            nameMap.put("#id", "productID");
            HashMap<String, Object> valueMap = new HashMap<String, Object>();

            valueMap.put(":i", bookId);
            QuerySpec bookQuerySpec = new QuerySpec()
                    .withKeyConditionExpression("#id = :i")
                    .withNameMap(nameMap)
                    .withValueMap(valueMap);

            valueMap.put(":i", dvdId);
            QuerySpec dvdQuerySpec = new QuerySpec()
                    .withKeyConditionExpression("#id = :i")
                    .withNameMap(nameMap)
                    .withValueMap(valueMap);

            ItemCollection<QueryOutcome> bookQueryItems = table.query(bookQuerySpec);
            ItemCollection<QueryOutcome> dvdQueryItems = table.query(dvdQuerySpec);
            Iterator<Item> bookQueryIterator = bookQueryItems.iterator();
            Iterator<Item> dvdQueryIterator = dvdQueryItems.iterator();
            Item bookQueryItem = null, dvdQueryItem = null;

            System.out.println("Book");
            while (bookQueryIterator.hasNext()) {
                bookQueryItem = bookQueryIterator.next();
                System.out.println(bookQueryItem);
            }

            System.out.println("DVD");
            while (dvdQueryIterator.hasNext()) {
                dvdQueryItem = dvdQueryIterator.next();
                System.out.println(dvdQueryItem);
            }

            // Scan

            System.out.println("Scan");
            for (Item item : table.scan(new ScanSpec())) {
                System.out.println(item);
            }

            // add 10000 items
            add10000Items(dynamoDB);
        } else {
            System.err.println("no table " + tableName);
        }
    }
}
