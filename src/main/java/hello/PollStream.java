package hello;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class PollStream implements Runnable {


    RedisTemplate<String,String> redisTemplate;

    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");

    int mapIndex;

    public PollStream(RedisTemplate<String,String> redisTemplate, int mapIndex) {
        this.redisTemplate = redisTemplate;
        this.mapIndex = mapIndex;
    }

    @Override
    public void run() {

        while(true) {

//            System.out.println("inside while-------------" + StreamController.pollQueue.size());



            for (Object streamMap : CoffeeLoader.pollList.get(mapIndex).entrySet()) {

//                System.out.println("inside for" + stream.getKey());

                Map.Entry<String, String> stream = (Map.Entry<String, String>) streamMap;

                String streamKey = stream.getKey();
                String readOffsetId = stream.getValue();

                List<MapRecord<String, Object, Object>> read = redisTemplate.opsForStream().read(StreamOffset.create(streamKey, ReadOffset.from(readOffsetId)));

                if (read.size() != 0) {

                    System.out.println("----------" + read.size());

                    CoffeeLoader.pollList.get(mapIndex).put(streamKey, read.get(read.size()-1).getId().getValue());
                    read.stream().forEach(t -> {

//                        System.out.println("******************************************");
//                        System.out.println("received time----" + CoffeeLoader.getTime());
//                        printThreadName("ThreadName--------");
//                        System.out.println(t.getId());
//                        System.out.println("Stream: " + t.getStream());
//                        System.out.println("Body: " + t.getValue().get("testmsg2"));
//                        System.out.println("******************************************");

                        if(t.getValue().containsKey("testmsg2")) {
                            try {

                                Date sent = sdf.parse(t.getValue().get("testmsg2").toString());
                                Date received = sdf.parse(CoffeeLoader.getTime());

//                                System.out.println(received.getTime() - sent.getTime());


                                long difference = Long.parseLong(System.getProperty("time-d"));

                                if(received.getTime() - sent.getTime() > difference) {

                                    System.out.println("******************************************");
                                    printThreadName("ThreadName--------");
                                    System.out.println("time difference " + (received.getTime() - sent.getTime()) );
                                    System.out.println("received time----" + CoffeeLoader.getTime());
                                    System.out.println(t.getId());
                                    System.out.println("Stream: " + t.getStream());
                                    System.out.println("Body: " + t.getValue().get("testmsg2"));
                                    System.out.println("******************************************");

                                }




                            } catch (ParseException e) {
                                System.out.println("Unable to parse--" + t.getValue().get("testmsg2").toString());
                            }
                        }



                    });
                }

            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public void printThreadName(String identifier) {

        Thread tw = Thread.currentThread();
        String namew = tw.getName();
        System.out.println(identifier + " thread=" + namew);

    }
}
