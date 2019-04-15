package hello;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StringRecord;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@RestController
public class StreamController {

    @Autowired
    ReactiveRedisTemplate<String,String> reactiveRedisTemplate;


    @Autowired
    RedisTemplate<String,String> redisTemplate;


    Map<String, String> publisherMap = new ConcurrentHashMap<>();
    Map<String, String> consumerMap = new ConcurrentHashMap<>();

    Map<String, String> consumingCurrentMap = new ConcurrentHashMap<>();

    ConcurrentLinkedQueue<Map.Entry<String,String>> publisherQueue = new ConcurrentLinkedQueue<>();

    Map<String, String> pollQueue = new ConcurrentHashMap<>();




    boolean readingStream = false;

    boolean addingToPublisher = false;


    public void addItemToPollQueue(String stream) {
        if(!pollQueue.containsKey(stream)) {
            // add stream to start getting messages from the start
            pollQueue.put(stream, "0");
        }
    }


    public void pollStreams() {

        // infinite loop to poll all the streams
        while(true) {

//            System.out.println("inside while-------------" + pollQueue.size());

            for (Map.Entry<String, String> stream : pollQueue.entrySet()) {

//                System.out.println("inside for");

                String streamKey = stream.getKey();
                String readOffsetId = stream.getValue();

                List<MapRecord<String, Object, Object>> read = redisTemplate.opsForStream().read(StreamOffset.create(streamKey, ReadOffset.from(readOffsetId)));

                if (read.size() != 0) {

                System.out.println("----------" + read.size());

                    pollQueue.put(streamKey, read.get(read.size()-1).getId().getValue());
                    read.stream().forEach(t -> {

                        System.out.println("inside-do-on-next");
                        System.out.println(t.getId());
                        System.out.println("Stream: " + t.getStream());
                        System.out.println("Body: " + t.getValue());

                    });
                }





            }
        }

    }


    @GetMapping("/start-polling-streams")
    public void startPolling() {

        addItemToPollQueue("my-stream-one");
        addItemToPollQueue("my-stream-two");
//        addItemToPollQueue("my-stream-three");
//        addItemToPollQueue("my-stream-four");

        pollStreams();

    }

//    public void publishStreamData() {
//
//        Map<String, String> testMap = new HashMap<>();
//        testMap.put("testmsg1", "testmsg1");
//
//        for(int i=0; i<10; i++) {
//            String streamKey = "my-stream-" + i;
//            StringRecord records = StreamRecords.string(testMap).withStreamKey(streamKey);
//
//            reactiveRedisTemplate.opsForStream()
//                    .add(records)
//                    .doOnNext(t -> {
//                        printThreadName("in mono doInNext-------------");
//                        System.out.println("pushing to " + streamKey + "  " + t.getValue() + " currently processing  " + readingStream + " time -- " + CoffeeLoader.getTime() );
//
//                        Map.Entry<String, String> publishedRecord = new AbstractMap.SimpleEntry<>(streamKey, t.getValue());
//                        publisherQueue.add(publishedRecord);
//
//                    })
//                    .subscribe();
//
//        }
//
//        consumeStreamData();
//
//    }
//
//    public void consumeStreamData() {
//
////        while()
//    }

//    @GetMapping("/add-stream-data")
//    public void addStreamData() {
//
//        Map<String, String> testMap = new HashMap<>();
//        testMap.put("testmsg1", "testmsg1");
//
//        for(int i=0; i<10; i++) {
//            String streamKey = "my-stream-" + i;
//
//            printThreadName("in publish for-------------");
//
//            StringRecord records = StreamRecords.string(testMap).withStreamKey(streamKey);
//            reactiveRedisTemplate.opsForStream().add(records)
//                    .doOnNext(t -> {
//                        printThreadName("in mono doInNext-------------");
//                        System.out.println("pushing to " + streamKey + "  " + t.getValue() + " currently processing  " + readingStream + " time -- " + CoffeeLoader.getTime() );
////                        if(!addingToPublisher) {
//                            publisherMap.put(t.getValue(), streamKey);
////                        }
//
//                    }).subscribe();
//
//            if (!readingStream) {
//                readStreams();
//            }
//
//        }
//
//
//    }

//    public void readStreams() {
//
//        while(true) {
//
//            readingStream = true;
//            printThreadName("in while-------------");
//
//            for (Map.Entry<String, String> stream : publisherMap.entrySet()) {
//
//                String streamKey = stream.getValue();
//                String readOffsetId = consumerMap.getOrDefault(streamKey, "0");
//
//
//                if(consumingCurrentMap.containsKey(streamKey)){
//                    consumingCurrentMap.put(streamKey, stream.getKey());
//                } else {
//
//                    if (!stream.getKey().equals(readOffsetId)) {
//                        Flux<MapRecord<String, Object, Object>> read = reactiveRedisTemplate.opsForStream()
//                                .read(StreamOffset.create(streamKey, ReadOffset.from(readOffsetId)))
//                                .doOnSubscribe(at -> {
//                                    consumingCurrentMap.put(streamKey, stream.getKey());
//                                })
//                                .doOnNext(t -> {
//
//                                    try {
//                                        int i = getRandomNumberInRange(1,5);
//                                        System.out.println(i);
//                                        Thread.sleep(i);
//                                    } catch (InterruptedException e) {
//                                        e.printStackTrace();
//                                    }
//
//                                    String streamName = t.getStream();
//
//                                    System.out.println(t.getId());
//                                    System.out.println("Stream: " + t.getStream());
//                                    System.out.println("Body: " + t.getValue());
//                                    printThreadName("in doOnNext-------------");
//                                    consumerMap.put(streamName, t.getId().getValue());
//                                    if (publisherMap.containsKey(stream.getKey())) {
//                                        publisherMap.remove(stream.getKey());
//                                    }
//                                    if (consumingCurrentMap.containsKey(streamName)) {
//                                        if (consumingCurrentMap.get(streamName).equals(t.getId())) {
//                                            consumingCurrentMap.remove(streamName);
//                                        }
//                                    }
//                                });
//                        read.subscribe();
//                    }
//                }
//
//
//            }
//        }
//
//        readingStream = false;
//
//        System.out.println("reading is done");
//
//    }

    public void printThreadName(String identifier) {

        Thread tw = Thread.currentThread();
        String namew = tw.getName();
        System.out.println(identifier + " thread=" + namew);

    }

    private static int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

}
