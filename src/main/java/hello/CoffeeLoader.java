package hello;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamReceiver;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeoutException;

@Component
public class CoffeeLoader {

    @Autowired
    private ReactiveRedisConnectionFactory factory;

    @Autowired
    RedisTemplate<String, String> redisTemplate;
//    private final ReactiveRedisOperations<String, Coffee> coffeeOps;

//    public CoffeeLoader(@Qualifier("reactiveRedisConnectionFactory") ReactiveRedisConnectionFactory factory, ReactiveRedisOperations<String, Coffee> coffeeOps) {
//        this.factory = factory;
//        this.coffeeOps = coffeeOps;
//    }

    static List<Map> pollList = new ArrayList<>();

    static Iterator myRoundRobin;

    @PostConstruct
    public void pollStreams() {

        pollList.add(new ConcurrentHashMap<String, String>());
        pollList.add(new ConcurrentHashMap<String, String>());
        pollList.add(new ConcurrentHashMap<String, String>());
        pollList.add(new ConcurrentHashMap<String, String>());
        pollList.add(new ConcurrentHashMap<String, String>());

        myRoundRobin = new MyRoundRobin(CoffeeLoader.pollList).iterator();



        PollStream pollStream0 = new PollStream(redisTemplate, 0);
        PollStream pollStream1 = new PollStream(redisTemplate, 1);
        PollStream pollStream2 = new PollStream(redisTemplate, 2);
        PollStream pollStream3 = new PollStream(redisTemplate, 3);
        PollStream pollStream4 = new PollStream(redisTemplate, 4);

        Thread thread0 = new Thread(pollStream0);
        thread0.start();

        Thread thread1 = new Thread(pollStream1);
        thread1.start();

        Thread thread2 = new Thread(pollStream2);
        thread2.start();

        Thread thread3 = new Thread(pollStream3);
        thread3.start();

        Thread thread4 = new Thread(pollStream4);
        thread4.start();




        System.out.println("started thread for polling");

    }

        @PostConstruct
    public void loadData() {
        System.out.println("herere also--------------");
//        factory.getReactiveConnection().serverCommands().flushAll().thenMany(
//                Flux.just("Jet Black Redis", "Darth Redis", "Black Alert Redis")
//                        .map(name -> new Coffee(UUID.randomUUID().toString(), name))
//                        .flatMap(coffee -> coffeeOps.opsForValue().set(coffee.getId(), coffee)))
//                .thenMany(coffeeOps.keys("*")
//                        .flatMap(coffeeOps.opsForValue()::get))
//                .subscribe(System.out::println);


//        StreamReceiver.StreamReceiverOptions<String, MapRecord<String, String, String>> options = StreamReceiver.StreamReceiverOptions.builder().pollTimeout(Duration.ofMillis(100))
//                .build();







//        Flux<MapRecord<String, String, String>> messages = receiver
//                .receive(StreamOffset.fromStart("my-stream"))
//                .doOnNext(it -> {
//                    System.out.println("MessageId: " + it.getId());
//                    System.out.println("Stream: " + it.getStream());
//                    System.out.println("Body: " + it.getValue());
//                });
//
//        messages.subscribe();
//
//



//        Thread t = Thread.currentThread();
//        String name = t.getName();
//        System.out.println("name outside =" + name);
//
//        StreamReceiver<String, MapRecord<String, String, String>> receiver = StreamReceiver.create(factory);
//
//
//        System.out.println("before subscribing" + getTime());
//
//
//        Map<String, String> publisherMap = new ConcurrentHashMap<>();
//        Map<String, String> consumerMap = new ConcurrentHashMap<>();
//
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
////        receiver
////        .receive(StreamOffset.fromStart("my-stream-one"))
////        .doOnNext(it -> {
////            System.out.println("MessageId: " + it.getId());
////            System.out.println("Stream: " + it.getStream());
////            System.out.println("Body: " + it.getValue());
////        })
////        .timeout(Duration.ofMillis(1000))
////        .doOnError(TimeoutException.class, e ->System.out.println("timeout" + getTime() + " --- Timed out"))
////        .doOnError(Exception.class, e ->System.out.println("Exception " + getTime() + " ------------------------------------------- Exception"))
////        .doOnComplete(() -> System.out.println("complete"))
////        .doOnCancel(() -> System.out.println("cancelled" + getTime() + "cancelled"))
////        .subscribe();
////
////        receiver
////        .receive(StreamOffset.fromStart("my-stream-two"))
////        .doOnNext(it -> {
////            System.out.println("MessageId: " + it.getId());
////            System.out.println("Stream: " + it.getStream());
////            System.out.println("Body: " + it.getValue());
////        })
////        .timeout(Duration.ofMillis(1000))
////        .doOnError(TimeoutException.class, e ->System.out.println("timeout" + getTime() + " --- Timed out"))
////        .doOnError(Exception.class, e ->System.out.println("Exception " + getTime() + " ------------------------------------------- Exception"))
////        .doOnComplete(() -> System.out.println("complete"))
////        .doOnCancel(() -> System.out.println("cancelled" + getTime() + "cancelled"))
////        .subscribe();
//
////        int connectionPool = 2;
////        int currentConnections = 0;
//
//        Map<String,String> mp = new ConcurrentHashMap<>();
////
//        while(!publisherMap.isEmpty()) {
//
//            Thread tw = Thread.currentThread();
//            String namew = tw.getName();
//            System.out.println("name in while=" + namew);
//
//            System.out.println("in while size------------" + publisherMap.size());
//
//
//            for (Map.Entry<String,String> entry : publisherMap.entrySet()){
//
//
//                Thread tf = Thread.currentThread();
//                String namef = tf.getName();
//                System.out.println("name in for=" + namef);
//
//                System.out.println("in for entry---------------" + entry.getKey());
//
//                String entryStreamKey = entry.getKey();
//
//
//
//
//
//                System.out.println(mp.containsKey(entryStreamKey));
//
//                if(!mp.containsKey(entryStreamKey)) {
//
//                    System.out.println("stream key exists---------------");
//
//                    receiver
//                    .receive(StreamOffset.create(entryStreamKey, ReadOffset.from(consumerMap.getOrDefault(entryStreamKey, "0"))))
//                            .doOnSubscribe(at -> {
//                                mp.put(entryStreamKey, "");
//                            })
//                    .doOnNext(it -> {
//
//                        Thread td = Thread.currentThread();
//                        String named = td.getName();
//                        System.out.println("name=" + named);
//
//                        consumerMap.put(entryStreamKey, it.getId().getValue());
//
//                        System.out.println("do onnnnnnn next--" + getTime());
//                        System.out.println("MessageId: " + it.getId());
//                        System.out.println("Stream: " + it.getStream());
//                        System.out.println("Body: " + it.getValue());
//
//                        if(publisherMap.get(entryStreamKey).equals(it.getId().getValue())) {
//                            System.out.println("Modified----------------cuncurrent hashmap");
//                            publisherMap.remove(entryStreamKey);
//                        }
//                    })
//                    .timeout(Duration.ofMillis(1000))
//                    .doOnError(TimeoutException.class, e -> {
//                        System.out.println("timeout" + getTime() + " --- Timed out");
//                        mp.remove(entryStreamKey);
//                    })
//                    .doOnError(Exception.class, e -> {
//                        System.out.println("Exception " + getTime() + " ------------------------------------------- Exception");
//                        mp.remove(entryStreamKey);
//                        e.printStackTrace();
//                    })
//                    .doOnComplete(() -> System.out.println("complete"))
//                    .doOnCancel(() -> System.out.println("cancelled" + getTime() + "cancelled"))
//                    .subscribe();
//
//                }
//
//            }
//
//
//
//        }

//        Queue<Map> publisherQueue = new LinkedList<>();


//        receiver.receive(StreamOffset.create("test", ReadOffset.from("123")));




//        Disposable disposable = messages1.subscribe();
//
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }



//        System.out.println(System.currentTimeMillis() +  "----" +Runtime.getRuntime().availableProcessors());

//       Disposable disposable1 = messages1.subscribe();



//*********************Enable this to subscribe to multiple connections at a time********************************

//       for(int i=0; i<10; i++) {
//
//            receiver
//                    .receive(StreamOffset.fromStart("my-stream-" + i))
//                    .doOnNext(it -> {
//                        System.out.println("MessageId: " + it.getId());
//                        System.out.println("Stream: " + it.getStream());
//                        System.out.println("Body: " + it.getValue());
//                    }).subscribe();
//
//           try {
//               Thread.sleep(8000);
//           } catch (InterruptedException e) {
//               e.printStackTrace();
//           }
//
//       }

//
//        try {
//            Thread.sleep(10000);
////
////
//////            Thread.sleep(10000);
////            disposable.dispose();
////
////
////            Thread.sleep(5000);
////
////
//            Flux<MapRecord<String, String, String>> messages2 = receiver
//                    .receive(StreamOffset.fromStart("my-stream-two"))
//                    .doOnNext(it -> {
//                        System.out.println("MessageId: " + it.getId());
//                        System.out.println("Stream: " + it.getStream());
//                        System.out.println("Body: " + it.getValue());
//                    });
//////
//////
//////
//////            System.out.println(Runtime.getRuntime().availableProcessors());
//////
//            Disposable disposable2 = messages2.subscribe();
//////
//////            Thread.sleep(10000);
//////            disposable2.dispose();
////
////
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }




//***************************************************************************************************************
//
//        Flux<MapRecord<String, String, String>> messages2 = receiver
//                .receive(StreamOffset.fromStart("my-stream-1"))
//                .doOnNext(it -> {
//                    System.out.println("MessageId: " + it.getId());
//                    System.out.println("Stream: " + it.getStream());
//                    System.out.println("Body: " + it.getValue());
//                });
//
//        messages2.subscribe();
//
//
//        Flux<MapRecord<String, String, String>> messages3 = receiver
//                .receive(StreamOffset.fromStart("my-stream-three"))
//
//
//
//                .doOnNext(it -> {
//                    System.out.println("MessageId: " + it.getId());
//                    System.out.println("Stream: " + it.getStream());
//                    System.out.println("Body: " + it.getValue());
//                });
//
//        messages3.subscribe();
//
//        Flux<MapRecord<String, String, String>> messages4 = receiver
//                .receive(StreamOffset.fromStart("my-stream-four"))
//                .doOnNext(it -> {
//                    System.out.println("MessageId: " + it.getId());
//                    System.out.println("Stream: " + it.getStream());
//                    System.out.println("Body: " + it.getValue());
//                });
//
//        Disposable subscribe = messages4.subscribe();
//        subscribe.dispose();



    }

    public static String getTime() {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        return sdf.format(cal.getTime()) ;
    }
}
