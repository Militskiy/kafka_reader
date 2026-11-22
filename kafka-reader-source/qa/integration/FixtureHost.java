import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.resource.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Disposable local broker fixture. No fixture mutation code is included in either desktop app. */
public final class FixtureHost {
    static final ObjectMapper JSON = new ObjectMapper();
    static final Map<String,KafkaProducer<byte[],byte[]>> transactions = new HashMap<>();
    static final Map<String,Long> epochs = new HashMap<>();
    static Admin admin;
    static KafkaProducer<byte[],byte[]> producer;
    static String clientEndpoint;

    static Properties authenticated(String user, String password) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", clientEndpoint);
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                user + "\" password=\"" + password + "\";");
        return properties;
    }

    static String verifyCluster(Admin client, String expected) throws Exception {
        String actual = client.describeCluster().clusterId().get(45, TimeUnit.SECONDS);
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Listener belongs to a different fixture cluster");
        }
        return actual;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        String expectedCluster = args[0];
        String plainEndpoint = "127.0.0.1:" + Integer.parseInt(args[1]);
        clientEndpoint = "127.0.0.1:" + Integer.parseInt(args[2]);
        try (var plain = Admin.create(Map.of("bootstrap.servers", plainEndpoint))) {
            verifyCluster(plain, expectedCluster);
        }
        admin = Admin.create(authenticated("admin", System.getenv("KAFKA_FIXTURE_ADMIN_PASSWORD")));
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String cluster = verifyCluster(admin, expectedCluster);
            producer = producer(null);
            System.out.println(JSON.writeValueAsString(Map.of("clusterId", cluster)));
            System.out.flush();
            String line;
            while ((line = in.readLine()) != null) {
                JsonNode request = JSON.readTree(line);
                String op = request.path("op").asText();
                String app = request.path("app").asText("csharp");
                try {
                    Object response = switch (op) {
                        case "seed" -> seed(app);
                        case "append" -> {
                            String value = request.path("value").asText("late-arrival");
                            int partition = request.path("partition").asInt(0);
                            long offset = send(producer, events(app), partition, value).offset();
                            yield Map.of("ok", true, "offset", offset);
                        }
                        case "open" -> {
                            KafkaProducer<byte[],byte[]> tx = transactions.get(app);
                            tx.beginTransaction(); send(tx, txTopic(app), 0, "still-open-transaction");
                            yield Map.of("ok", true);
                        }
                        case "abort" -> { transactions.get(app).abortTransaction(); yield Map.of("ok", true); }
                        case "state" -> state(app);
                        case "group-denied" -> Map.of("denied", groupDenied(app));
                        default -> throw new IllegalArgumentException("Unknown fixture command");
                    };
                    System.out.println(JSON.writeValueAsString(response));
                } catch (Exception ex) {
                    System.out.println(JSON.writeValueAsString(Map.of("error", ex.toString())));
                }
                System.out.flush();
            }
        } finally {
            for (var tx : transactions.values()) tx.close(Duration.ofSeconds(3));
            if (producer != null) producer.close(Duration.ofSeconds(3));
            admin.close(Duration.ofSeconds(3));
        }
    }

    static Object seed(String app) throws Exception {
        long epoch = System.currentTimeMillis()-120_000;
        epochs.put(app, epoch);
        admin.createTopics(List.of(new NewTopic(events(app),3,(short)1),new NewTopic(txTopic(app),1,(short)1),new NewTopic(forbidden(app),1,(short)1))).all().get(20,TimeUnit.SECONDS);
        List<AclBinding> acls = new ArrayList<>();
        for (String topic : List.of(events(app),txTopic(app))) for (AclOperation op : List.of(AclOperation.READ,AclOperation.DESCRIBE))
            acls.add(new AclBinding(new ResourcePattern(ResourceType.TOPIC,topic,PatternType.LITERAL),new AccessControlEntry("User:reader","*",op,AclPermissionType.ALLOW)));
        admin.createAcls(acls).all().get(20,TimeUnit.SECONDS);
        for (int p=0;p<3;p++) for (int n=0;n<20;n++) {
            byte[] value=n==5?null:n==6?new byte[0]:n==7?new byte[]{0,(byte)255,2}:bytes("{\"partition\":"+p+",\"index\":"+n+",\"label\":\"Москва ✓\"}");
            var message=new ProducerRecord<byte[],byte[]>(events(app),p,epoch+n*1000L,n==4?null:bytes("key-"+n),value,
                List.of(new RecordHeader("tag",bytes("first")),new RecordHeader("tag",bytes("second")),new RecordHeader("nullable",null),new RecordHeader("empty",new byte[0])));
            producer.send(message).get(20,TimeUnit.SECONDS);
        }
        admin.alterConsumerGroupOffsets(group(app),Map.of(new TopicPartition(events(app),0),new OffsetAndMetadata(7))).all().get(20,TimeUnit.SECONDS);
        var tx=producer("fixture-transaction-"+app); transactions.put(app,tx); tx.initTransactions();
        tx.beginTransaction(); send(tx,txTopic(app),0,"committed-1"); send(tx,txTopic(app),0,"committed-2"); tx.commitTransaction();
        tx.beginTransaction(); send(tx,txTopic(app),0,"aborted-1"); send(tx,txTopic(app),0,"aborted-2"); tx.abortTransaction();
        send(producer,txTopic(app),0,"nontransactional");
        return Map.of("ok",true,"epoch",epoch,"events",events(app),"transactions",txTopic(app),"forbidden",forbidden(app),"group",group(app));
    }
    static Object state(String app) throws Exception {
        Map<String,Long> offsets=new TreeMap<>();
        admin.listConsumerGroupOffsets(group(app)).partitionsToOffsetAndMetadata().get(20,TimeUnit.SECONDS).forEach((p,o)->offsets.put(p.topic()+":"+p.partition(),o.offset()));
        Set<String> groups=new TreeSet<>();
        admin.listConsumerGroups().all().get(20,TimeUnit.SECONDS).forEach(g->groups.add(g.groupId()));
        return Map.of("offsets",offsets,"groups",groups);
    }
    static boolean groupDenied(String app) {
        Properties p=authenticated("reader", System.getenv("KAFKA_FIXTURE_READER_PASSWORD"));
        p.setProperty("group.id","denied-"+app);p.setProperty("enable.auto.commit","false");
        p.setProperty("key.deserializer",ByteArrayDeserializer.class.getName());p.setProperty("value.deserializer",ByteArrayDeserializer.class.getName());
        try (var c=new KafkaConsumer<byte[],byte[]>(p)) {
            var tp=new TopicPartition(events(app),0);c.assign(List.of(tp));c.commitSync(Map.of(tp,new OffsetAndMetadata(1)),Duration.ofSeconds(10));return false;
        } catch(GroupAuthorizationException expected) { return true; }
    }
    static KafkaProducer<byte[],byte[]> producer(String id) {
        Properties p=authenticated("admin", System.getenv("KAFKA_FIXTURE_ADMIN_PASSWORD"));
        p.setProperty("key.serializer",ByteArraySerializer.class.getName());p.setProperty("value.serializer",ByteArraySerializer.class.getName());p.setProperty("acks","all");p.setProperty("max.block.ms","20000");
        if(id!=null)p.setProperty("transactional.id",id);return new KafkaProducer<>(p);
    }
    static RecordMetadata send(KafkaProducer<byte[],byte[]> p,String topic,int partition,String value) throws Exception {
        return p.send(new ProducerRecord<>(topic,partition,bytes("fixture"),bytes(value))).get(20,TimeUnit.SECONDS);
    }
    static byte[] bytes(String s){return s.getBytes(StandardCharsets.UTF_8);}
    static String events(String a){return "reader-"+a+"-events";}
    static String txTopic(String a){return "reader-"+a+"-transactions";}
    static String forbidden(String a){return "reader-"+a+"-forbidden";}
    static String group(String a){return "existing-"+a+"-group";}
}
