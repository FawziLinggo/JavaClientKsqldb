import io.confluent.ksql.api.client.*;

import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class App {
    private static Properties props = new Properties();

    public static void main(String[] args) throws ExecutionException, InterruptedException,Exception {
        props.load(new FileReader("src/main/resources/ksqlclient.properties"));

        String host = (String) props.get("hostname");
        int port = Integer.parseInt((String) props.get("port"));
        String truststore = (String) props.get("truststore");
        String keystore = (String) props.get("keystore");
        String passtruststore = (String) props.get("pass.truststore");
        String passkeystore = (String) props.get("pass.keystore");
        String username = (String) props.get("username");
        String password = (String) props.get("password");
        boolean setUseTls = Boolean.parseBoolean ((String) props.get("setUseTls"));
        boolean setUseAlpn = Boolean.parseBoolean ((String) props.get("setUseAlpn"));

        final ClientOptions clientOptions = ClientOptions.create()
                .setHost(host)
                .setPort(port)
                .setTrustStore(truststore)
                .setTrustStorePassword(passtruststore)
                .setKeyStore(keystore)
                .setKeyStorePassword(passkeystore)
                .setBasicAuthCredentials(username,password)
                .setUseTls(setUseTls)
                .setUseAlpn(setUseAlpn);
        final Client client = Client.create(clientOptions);


        final List<TopicInfo> topicInfos = client.listTopics() .get();
        topicInfos.forEach(System.out::println); // List Topic
        client.listTables().get().forEach(System.out::println); // List Table


        final Map<String, Object> properties = Map.of("auto.offset.reset", "earliest");

        final StreamedQueryResult streamedQueryResult = client.streamQuery("SELECT * FROM TES_DEV_TO_TOPIC_3 EMIT CHANGES;",properties).get();

        // While
        while (true){
            Row row = streamedQueryResult.poll();
            if (row != null) {
                System.out.println("Received a row!");
                System.out.println("Row: " + row.values());
            } else {
                System.out.println("Query has ended.");
            }
        }


        // For
//        for (int i = 0; i < 10; i++) {
//            // Block until a new row is available
//            Row row = streamedQueryResult.poll();
//            if (row != null) {
//                System.out.println("Received a row!");
//                System.out.println("Row: " + row.values());
//            } else {
//                System.out.println("Query has ended.");
//            }
//        }

//        client.close();

    }
}
