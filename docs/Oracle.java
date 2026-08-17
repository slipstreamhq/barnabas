import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;

public class Oracle {
    static void run(AbstractPartitionAssignor a, Map<String,Integer> parts, Map<String,List<String>> subs) {
        Map<String, ConsumerPartitionAssignor.Subscription> s = new LinkedHashMap<>();
        for (Map.Entry<String,List<String>> e : subs.entrySet())
            s.put(e.getKey(), new ConsumerPartitionAssignor.Subscription(e.getValue()));
        Map<String, List<TopicPartition>> out = a.assign(parts, s);
        List<String> keys = new ArrayList<>(out.keySet());
        Collections.sort(keys);
        StringBuilder sb = new StringBuilder(a.name()).append(" ");
        for (String k : keys) {
            List<TopicPartition> tps = new ArrayList<>(out.get(k));
            tps.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));
            sb.append(k).append("=[");
            for (TopicPartition tp : tps) sb.append(tp.topic()).append(":").append(tp.partition()).append(",");
            sb.append("] ");
        }
        System.out.println(sb.toString());
    }
    public static void main(String[] args) {
        // case 1: 3 partitions, 2 members
        run(new RangeAssignor(), Map.of("t",3), Map.of("c0",List.of("t"),"c1",List.of("t")));
        run(new RoundRobinAssignor(), Map.of("t",3), Map.of("c0",List.of("t"),"c1",List.of("t")));
        // case 2: two topics of 3, two members  (the lopsided case)
        Map<String,Integer> two = new TreeMap<>(); two.put("a",3); two.put("b",3);
        run(new RangeAssignor(), two, Map.of("c0",List.of("a","b"),"c1",List.of("a","b")));
        run(new RoundRobinAssignor(), two, Map.of("c0",List.of("a","b"),"c1",List.of("a","b")));
        // case 3: uneven subscriptions
        Map<String,Integer> ab = new TreeMap<>(); ab.put("a",2); ab.put("b",2);
        run(new RoundRobinAssignor(), ab, Map.of("c0",List.of("a"),"c1",List.of("a","b")));
        // case 4: the 5/3 mixed case from our property test
        Map<String,Integer> mix = new TreeMap<>(); mix.put("a",5); mix.put("b",3);
        run(new RangeAssignor(), mix, Map.of("c0",List.of("a","b"),"c1",List.of("a","b"),"c2",List.of("a")));
        run(new RoundRobinAssignor(), mix, Map.of("c0",List.of("a","b"),"c1",List.of("a","b"),"c2",List.of("a")));
    }
}
