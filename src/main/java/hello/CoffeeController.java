package hello;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveStreamCommands;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@RestController
public class CoffeeController {
    private final ReactiveRedisOperations<String, Coffee> coffeeOps;

    @Autowired
    private ReactiveRedisConnectionFactory factory;

    @Autowired
    RedisTemplate<String, String> redisTemplate;

    @Autowired
    ReactiveRedisTemplate<String,String> reactiveRedisTemplate;


    Map<String, String> publisherMap = new ConcurrentHashMap<>();
    Map<String, String> consumerMap = new ConcurrentHashMap<>();

    Map<String,String> mp = new ConcurrentHashMap<>();

    CoffeeController(ReactiveRedisOperations<String, Coffee> coffeeOps) {
        this.coffeeOps = coffeeOps;
    }

    int connections = 1;

    public boolean all() {


//        StreamReceiver<String, MapRecord<String, String, String>> receiver = StreamReceiver.create(factory);
//
//        Flux<MapRecord<String, String, String>> messages2 = receiver
//                .receive(StreamOffset.fromStart("my-stream-two"))
//                .doOnNext(it -> {
//                    System.out.println("MessageId: " + it.getId());
//                    System.out.println("Stream: " + it.getStream());
//                    System.out.println("Body: " + it.getValue());
//                });
//
//        disposable = messages2.subscribe();

        Thread t = Thread.currentThread();
        String name = t.getName();
        System.out.println("name outside =" + name);

        StreamReceiver<String, MapRecord<String, String, String>> receiver = StreamReceiver.create(factory);


        System.out.println("before subscribing" + CoffeeLoader.getTime());



//        DefaultReactiveScriptExecutor


//        receiver
//        .receive(StreamOffset.fromStart("my-stream-one"))
//        .doOnNext(it -> {
//            System.out.println("MessageId: " + it.getId());
//            System.out.println("Stream: " + it.getStream());
//            System.out.println("Body: " + it.getValue());
//        })
//        .timeout(Duration.ofMillis(1000))
//        .doOnError(TimeoutException.class, e ->System.out.println("timeout" + getTime() + " --- Timed out"))
//        .doOnError(Exception.class, e ->System.out.println("Exception " + getTime() + " ------------------------------------------- Exception"))
//        .doOnComplete(() -> System.out.println("complete"))
//        .doOnCancel(() -> System.out.println("cancelled" + getTime() + "cancelled"))
//        .subscribe();
//
//        receiver
//        .receive(StreamOffset.fromStart("my-stream-two"))
//        .doOnNext(it -> {
//            System.out.println("MessageId: " + it.getId());
//            System.out.println("Stream: " + it.getStream());
//            System.out.println("Body: " + it.getValue());
//        })
//        .timeout(Duration.ofMillis(1000))
//        .doOnError(TimeoutException.class, e ->System.out.println("timeout" + getTime() + " --- Timed out"))
//        .doOnError(Exception.class, e ->System.out.println("Exception " + getTime() + " ------------------------------------------- Exception"))
//        .doOnComplete(() -> System.out.println("complete"))
//        .doOnCancel(() -> System.out.println("cancelled" + getTime() + "cancelled"))
//        .subscribe();

//        int connectionPool = 2;
//        int currentConnections = 0;
//
        while(!publisherMap.isEmpty()) {

            Thread tw = Thread.currentThread();
            String namew = tw.getName();
//            System.out.println("name in while=" + namew);

//            System.out.println("in while size------------" + publisherMap.size());


            for (Map.Entry<String,String> entry : publisherMap.entrySet()){


                Thread tf = Thread.currentThread();
                String namef = tf.getName();
//                System.out.println("name in for=" + namef);

//                System.out.println("in for entry---------------" + entry.getKey());

                String entryStreamKey = entry.getKey();





//                System.out.println(mp.containsKey(entryStreamKey));

                if(!mp.containsKey(entryStreamKey) && mp.size() < connections) {

//                    System.out.println("stream key exists---------------");

//                    try {
//                        Thread.sleep(1020);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }


                    receiver
                            .receive(StreamOffset.create(entryStreamKey, ReadOffset.from(consumerMap.getOrDefault(entryStreamKey, "0"))))
                            .doOnSubscribe(at -> {

                                System.out.println("subscribing to " + entryStreamKey);
                                mp.put(entryStreamKey, "");
                            })
                            .doOnNext(it -> {

                                Thread td = Thread.currentThread();
                                String named = td.getName();
                                System.out.println("name=" + named);

                                consumerMap.put(entryStreamKey, it.getId().getValue());

                                System.out.println("do onnnnnnn next--" + CoffeeLoader.getTime());
                                System.out.println("MessageId: " + it.getId());
                                System.out.println("Stream: " + it.getStream());
                                System.out.println("Body: " + it.getValue());

                                if(publisherMap.get(entryStreamKey).equals(it.getId().getValue())) {
                                    System.out.println("Modified----------------cuncurrent hashmap");
                                    publisherMap.remove(entryStreamKey);
                                }
                            })
                            .timeout(Duration.ofMillis(1000))
                            .doOnError(TimeoutException.class, e -> {
                                Thread td = Thread.currentThread();
                                String named = td.getName();
                                System.out.println("name=" + named);
                                System.out.println("timeout" + CoffeeLoader.getTime() + " --- Timed out");
                                mp.remove(entryStreamKey);
                            })
                            .doOnError(Exception.class, e -> {

                                Thread td = Thread.currentThread();
                                String named = td.getName();
                                System.out.println("name=" + named);
                                System.out.println("Exception " + CoffeeLoader.getTime() + " ------------------------------------------- Exception");
                                System.out.println(entryStreamKey);
                                e.printStackTrace();
                                mp.remove(entryStreamKey);
                            })
                            .doOnComplete(() -> System.out.println("complete"))
                            .doOnCancel(() -> System.out.println("cancelled" + CoffeeLoader.getTime() + "cancelled"))
                            .subscribe();

                }

            }



        }

        return true;
    }

    @GetMapping("/coffee-brew")
    public boolean brew(String stream) {


        System.out.println(stream);

//        consumerMap = new ConcurrentHashMap<>();

//        Map<String, String> testMap = new HashMap<>();
//        testMap.put("testmsg1", "testmsg1");
//        String streamKey = "my-stream-three";
//
//        StringRecord records = StreamRecords.string(testMap).withStreamKey(streamKey);
//        RecordId recordId = redisTemplate.opsForStream().add(records);
//
//        publisherMap.put(streamKey, recordId.getValue());
//
//
//        String streamKey2 = "my-stream-two";
//
//        StringRecord records2 = StreamRecords.string(testMap).withStreamKey(streamKey2);
//        RecordId recordId2 = redisTemplate.opsForStream().add(records2);
//
//        publisherMap.put(streamKey2, recordId2.getValue());
//
//
//        String streamKey3 = "my-stream-four";
//
//        StringRecord records3 = StreamRecords.string(testMap).withStreamKey(streamKey3);
//        RecordId recordId3 = redisTemplate.opsForStream().add(records3);
//
//        publisherMap.put(streamKey3, recordId3.getValue());


//        List<MapRecord<String, Object, Object>> read = redisTemplate.opsForStream().read(StreamOffset.create("my-stream-one", ReadOffset.from("0")));
//
//        for(MapRecord<String, Object, Object> mr: read)  {
//            System.out.println(mr.getId());
//        }


//        Flux<MapRecord<String, Object, Object>> read1 = reactiveRedisTemplate.opsForStream().read(StreamOffset.create("my-stream-one", ReadOffset.from("0")));
//
//        read1.doOnNext(t -> {
//            System.out.println(t.getId().getValue());
//        }).subscribe();


//        Flux<MapRecord<String, Object, Object>> read2 = reactiveRedisTemplate.opsForStream().read(StreamOffset.create("my-stream-three", ReadOffset.from("0")));
//
//        read1.doOnNext(t -> {
//            System.out.println(t.getId().getValue());
//        }).subscribe();
//
//
//        Flux<MapRecord<String, Object, Object>> read3 = reactiveRedisTemplate.opsForStream().read(StreamOffset.create("my-stream-two", ReadOffset.from("0")));
//
//        read1.doOnNext(t -> {
//            System.out.println(t.getId().getValue());
//        }).subscribe();
//
//        Flux<MapRecord<String, Object, Object>> read4 = reactiveRedisTemplate.opsForStream().read(StreamOffset.create("my-stream-four", ReadOffset.from("0")));
//
//        read1.doOnNext(t -> {
//            System.out.println(t.getId().getValue());
//        }).subscribe();


        for(int i=0; i<100; i++) {
            Flux<MapRecord<String, Object, Object>> read = reactiveRedisTemplate.opsForStream().read(StreamOffset.create("my-stream-"+i, ReadOffset.from("0"))).doOnNext(t -> {
                System.out.println(t.getId());
                System.out.println("Stream: " + t.getStream());
                System.out.println("Body: " + t.getValue());

            });
            read.subscribe();
        }


//        all();
        return true;
    }

}
