package hello;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MyRoundRobin implements Iterable<Map> {
      private List<Map> coll;
      private int index = 0;

      public MyRoundRobin(List<Map> coll) { this.coll = coll; }

      public Iterator<Map> iterator() {
         return new Iterator<Map>() {


            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Map next() {
                index++;
                if(index>=coll.size()) {
                    index = 0;
                }
                Map res = coll.get(index);
                return res;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }
}