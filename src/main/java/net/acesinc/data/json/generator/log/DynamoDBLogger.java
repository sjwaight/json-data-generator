package net.acesinc.data.json.generator.log;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class DynamoDBLogger implements EventLogger {

    private static final Logger log = LogManager.getLogger(DynamoDBLogger.class);
    private final AmazonDynamoDB client;
    private final DynamoDB dynamoDB;
    private final Table table;
    private final ObjectMapper mapper = new ObjectMapper();

    public DynamoDBLogger(Map<String, Object> props) {
        String region = (String) props.get("region");
        String tableName = (String) props.get("tableName");
        String accessKey = (String) props.get("accessKey");
        String secretKey = (String) props.get("secretKey");

        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        this.client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();

        this.dynamoDB = new DynamoDB(client);
        this.table = dynamoDB.getTable(tableName);
    }

    @Override
    public void logEvent(String event, Map<String, Object> producerConfig) {
        try {
            JsonNode jsonNode = mapper.readTree(event);
            Map<String, Object> itemMap = mapper.convertValue(jsonNode, Map.class);
            PutItemRequest putItemRequest = new PutItemRequest()
                    .withTableName(table.getTableName())
                    .withItem(itemMap);
            PutItemResult putItemResult = client.putItem(putItemRequest);
            log.info("Document added to DynamoDB with result: " + putItemResult);
        } catch (Exception e) {
            log.error("Error inserting JSON data into DynamoDB", e);
        }
    }

    @Override
    public void shutdown() {
        if (client != null) {
            client.shutdown();
            log.info("DynamoDB client closed successfully");
        }
    }
}
