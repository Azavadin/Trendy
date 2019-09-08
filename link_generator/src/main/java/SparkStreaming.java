import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

/**
 * Created by das on 9/7/19.
 */
public class SparkStreaming
{
    public static void main (String[] args) throws Exception {

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("test");

        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(30));

        JavaInputDStream<ConsumerRecord<String, String>> dStream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

//        dStream.print();
        JavaDStream<String> uniqStrm = dStream.map(record -> (record.value()))
                .countByValue().map(r -> r._1);
//        uniqStrm.print();

        uniqStrm.map(r -> {
            String[] data = r.split(",");
            return data[1];
        }).countByValue().print(); // dstream().saveAsTextFiles("result", "");

//        PairFunction<String, String, String> splitFn = (s) -> {
//            String[] ss = s.split(",");
//            return new Tuple2<>(ss[1], ss[0]);
//        };
//        JavaPairDStream<String, String> dataStrm = uniqStrm.mapToPair(splitFn);
//        dataStrm.map(r -> "done map to pair... " + r._1() + " -- " + r._2()).print();
//        JavaPairDStream<String, Iterable<String>> result = dataStrm.groupByKey();
//        result.map(r -> {
//            String users = "";
//            for (String u : r._2()) {
//               users += u + ", ";
//            }
//            return r._1() + " -- [" + users + "]";
//        }).print();

        ssc.start();
        ssc.awaitTermination();
    }
}



