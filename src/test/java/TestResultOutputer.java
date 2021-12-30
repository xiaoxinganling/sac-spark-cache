import com.alibaba.fastjson.JSON;
import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import utils.CacheSketcher;
import utils.ResultOutputer;
import utils.SimpleUtil;

import java.io.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TestResultOutputer {
    @Test
    void testWriteHashMap() throws IOException {
        {
            Map<Long, Integer> integerMap = new HashMap<>();
            integerMap.put(1L, 1);
            integerMap.put(2L, 3);
            integerMap.put(5L, 7);
            integerMap.put(3L, 5);
            BufferedWriter bw = new BufferedWriter(new FileWriter("test_integer_map"));
            ResultOutputer.writeHashMap(bw, integerMap);
            bw.close();
            BufferedReader br = new BufferedReader(new FileReader("test_integer_map"));
            assertEquals("1 3 5 7", br.readLine());
            br.close();
        }
        {
            Map<Long, Long> longMap = new HashMap<>();
            longMap.put(1L, 1L);
            longMap.put(2L, 3L);
            longMap.put(4L, 6L);
            longMap.put(5L, 7L);
            longMap.put(3L, 5L);
            BufferedWriter bw = new BufferedWriter(new FileWriter("test_long_map"));
            ResultOutputer.writeHashMap(bw, longMap);
            bw.close();
            BufferedReader br = new BufferedReader(new FileReader("test_long_map"));
            assertEquals("1 3 5 6 7", br.readLine());
            br.close();
        }
        {
            Map<Long, Float> floatMap = new HashMap<>();
            floatMap.put(1L, 1.5f);
            floatMap.put(2L, 2.5f);
            floatMap.put(4L, 4.5f);
            floatMap.put(5L, 5.7f);
            floatMap.put(3L, 3.9f);
            BufferedWriter bw = new BufferedWriter(new FileWriter("test_float_map"));
            ResultOutputer.writeHashMap(bw, floatMap);
            bw.close();
            BufferedReader br = new BufferedReader(new FileReader("test_float_map"));
            assertEquals("1.5 2.5 3.9 4.5 5.7", br.readLine());
            br.close();
        }
    }

    @Test
    void testWriteFullSketchStatistics() throws Exception {
        JobStartEvent jse0 = JSON.parseObject("{\"Event\":\"SparkListenerJobStart\",\"Job ID\":0,\"Submission Time\":1636630209838,\"Stage Infos\":[{\"Stage ID\":0,\"Stage Attempt ID\":0,\"Stage Name\":\"count at SparkTC.scala:62\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.rdd.RDD.count(RDD.scala:1213)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:62)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]}],\"Stage IDs\":[0],\"Properties\":{}}\n")
                .toJavaObject(JobStartEvent.class);
        JobStartEvent jse1 = JSON.parseObject("{\"Event\":\"SparkListenerJobStart\",\"Job ID\":1,\"Submission Time\":1636630210525,\"Stage Infos\":[{\"Stage ID\":1,\"Stage Attempt ID\":0,\"Stage Name\":\"parallelize at SparkTC.scala:50\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.SparkContext.parallelize(SparkContext.scala:716)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:50)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":2,\"Stage Attempt ID\":0,\"Stage Name\":\"map at SparkTC.scala:58\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":1,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"1\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:58\",\"Parent IDs\":[0],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.rdd.RDD.map(RDD.scala:392)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:58)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":3,\"Stage Attempt ID\":0,\"Stage Name\":\"distinct at SparkTC.scala:70\",\"Number of Tasks\":2,\"RDD Info\":[{\"RDD ID\":7,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[6],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":4,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[3],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":6,\"Name\":\"UnionRDD\",\"Scope\":\"{\\\"id\\\":\\\"5\\\",\\\"name\\\":\\\"union\\\"}\",\"Callsite\":\"union at SparkTC.scala:70\",\"Parent IDs\":[0,5],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":5,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"4\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:70\",\"Parent IDs\":[4],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":3,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[2],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":2,\"Name\":\"CoGroupedRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[0,1],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[1,2],\"Details\":\"org.apache.spark.rdd.RDD.distinct(RDD.scala:427)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:70)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":4,\"Stage Attempt ID\":0,\"Stage Name\":\"count at SparkTC.scala:71\",\"Number of Tasks\":2,\"RDD Info\":[{\"RDD ID\":9,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[8],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":8,\"Name\":\"ShuffledRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[7],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[3],\"Details\":\"org.apache.spark.rdd.RDD.count(RDD.scala:1213)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:71)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]}],\"Stage IDs\":[1,2,3,4],\"Properties\":{}}\n")
                .toJavaObject(JobStartEvent.class);
        JobStartEvent jse2 = JSON.parseObject("{\"Event\":\"SparkListenerJobStart\",\"Job ID\":2,\"Submission Time\":1636630211337,\"Stage Infos\":[{\"Stage ID\":9,\"Stage Attempt ID\":0,\"Stage Name\":\"distinct at SparkTC.scala:70\",\"Number of Tasks\":2,\"RDD Info\":[{\"RDD ID\":9,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[8],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":8,\"Name\":\"ShuffledRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[7],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[7],\"Details\":\"org.apache.spark.rdd.RDD.distinct(RDD.scala:427)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:70)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":5,\"Stage Attempt ID\":0,\"Stage Name\":\"parallelize at SparkTC.scala:50\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.SparkContext.parallelize(SparkContext.scala:716)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:50)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":6,\"Stage Attempt ID\":0,\"Stage Name\":\"map at SparkTC.scala:58\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":1,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"1\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:58\",\"Parent IDs\":[0],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.rdd.RDD.map(RDD.scala:392)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:58)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":10,\"Stage Attempt ID\":0,\"Stage Name\":\"distinct at SparkTC.scala:70\",\"Number of Tasks\":4,\"RDD Info\":[{\"RDD ID\":15,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"22\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[14],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":11,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"19\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[10],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":10,\"Name\":\"CoGroupedRDD\",\"Scope\":\"{\\\"id\\\":\\\"19\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[9,1],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":14,\"Name\":\"UnionRDD\",\"Scope\":\"{\\\"id\\\":\\\"21\\\",\\\"name\\\":\\\"union\\\"}\",\"Callsite\":\"union at SparkTC.scala:70\",\"Parent IDs\":[9,13],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":8,\"Name\":\"ShuffledRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[7],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":9,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[8],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":12,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"19\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[11],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":13,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"20\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:70\",\"Parent IDs\":[12],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[9,7,8],\"Details\":\"org.apache.spark.rdd.RDD.distinct(RDD.scala:427)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:70)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":7,\"Stage Attempt ID\":0,\"Stage Name\":\"distinct at SparkTC.scala:70\",\"Number of Tasks\":2,\"RDD Info\":[{\"RDD ID\":7,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[6],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":4,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[3],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":6,\"Name\":\"UnionRDD\",\"Scope\":\"{\\\"id\\\":\\\"5\\\",\\\"name\\\":\\\"union\\\"}\",\"Callsite\":\"union at SparkTC.scala:70\",\"Parent IDs\":[0,5],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":5,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"4\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:70\",\"Parent IDs\":[4],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":3,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[2],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":2,\"Name\":\"CoGroupedRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[0,1],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[5,6],\"Details\":\"org.apache.spark.rdd.RDD.distinct(RDD.scala:427)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:70)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":11,\"Stage Attempt ID\":0,\"Stage Name\":\"count at SparkTC.scala:71\",\"Number of Tasks\":4,\"RDD Info\":[{\"RDD ID\":17,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"22\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[16],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":16,\"Name\":\"ShuffledRDD\",\"Scope\":\"{\\\"id\\\":\\\"22\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[15],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[10],\"Details\":\"org.apache.spark.rdd.RDD.count(RDD.scala:1213)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:71)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":8,\"Stage Attempt ID\":0,\"Stage Name\":\"map at SparkTC.scala:58\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":1,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"1\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:58\",\"Parent IDs\":[0],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.rdd.RDD.map(RDD.scala:392)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:58)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]}],\"Stage IDs\":[9,5,6,10,7,11,8],\"Properties\":{}}\n")
                .toJavaObject(JobStartEvent.class);
        JobStartEvent jse3 = JSON.parseObject("{\"Event\":\"SparkListenerJobStart\",\"Job ID\":3,\"Submission Time\":1636630211585,\"Stage Infos\":[{\"Stage ID\":15,\"Stage Attempt ID\":0,\"Stage Name\":\"map at SparkTC.scala:58\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":1,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"1\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:58\",\"Parent IDs\":[0],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.rdd.RDD.map(RDD.scala:392)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:58)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":12,\"Stage Attempt ID\":0,\"Stage Name\":\"parallelize at SparkTC.scala:50\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.SparkContext.parallelize(SparkContext.scala:716)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:50)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":16,\"Stage Attempt ID\":0,\"Stage Name\":\"distinct at SparkTC.scala:70\",\"Number of Tasks\":2,\"RDD Info\":[{\"RDD ID\":9,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[8],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":8,\"Name\":\"ShuffledRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[7],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[14],\"Details\":\"org.apache.spark.rdd.RDD.distinct(RDD.scala:427)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:70)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":13,\"Stage Attempt ID\":0,\"Stage Name\":\"map at SparkTC.scala:58\",\"Number of Tasks\":1,\"RDD Info\":[{\"RDD ID\":1,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"1\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:58\",\"Parent IDs\":[0],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[],\"Details\":\"org.apache.spark.rdd.RDD.map(RDD.scala:392)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:58)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":17,\"Stage Attempt ID\":0,\"Stage Name\":\"distinct at SparkTC.scala:70\",\"Number of Tasks\":4,\"RDD Info\":[{\"RDD ID\":15,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"22\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[14],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":11,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"19\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[10],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":10,\"Name\":\"CoGroupedRDD\",\"Scope\":\"{\\\"id\\\":\\\"19\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[9,1],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":14,\"Name\":\"UnionRDD\",\"Scope\":\"{\\\"id\\\":\\\"21\\\",\\\"name\\\":\\\"union\\\"}\",\"Callsite\":\"union at SparkTC.scala:70\",\"Parent IDs\":[9,13],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":8,\"Name\":\"ShuffledRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[7],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":9,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[8],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":12,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"19\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[11],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":13,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"20\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:70\",\"Parent IDs\":[12],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[15,16,14],\"Details\":\"org.apache.spark.rdd.RDD.distinct(RDD.scala:427)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:70)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":18,\"Stage Attempt ID\":0,\"Stage Name\":\"count at SparkTC.scala:76\",\"Number of Tasks\":4,\"RDD Info\":[{\"RDD ID\":17,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"22\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[16],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":16,\"Name\":\"ShuffledRDD\",\"Scope\":\"{\\\"id\\\":\\\"22\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[15],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":4,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[17],\"Details\":\"org.apache.spark.rdd.RDD.count(RDD.scala:1213)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:76)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]},{\"Stage ID\":14,\"Stage Attempt ID\":0,\"Stage Name\":\"distinct at SparkTC.scala:70\",\"Number of Tasks\":2,\"RDD Info\":[{\"RDD ID\":7,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"6\\\",\\\"name\\\":\\\"distinct\\\"}\",\"Callsite\":\"distinct at SparkTC.scala:70\",\"Parent IDs\":[6],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":4,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[3],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":6,\"Name\":\"UnionRDD\",\"Scope\":\"{\\\"id\\\":\\\"5\\\",\\\"name\\\":\\\"union\\\"}\",\"Callsite\":\"union at SparkTC.scala:70\",\"Parent IDs\":[0,5],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":2,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":0,\"Name\":\"ParallelCollectionRDD\",\"Scope\":\"{\\\"id\\\":\\\"0\\\",\\\"name\\\":\\\"parallelize\\\"}\",\"Callsite\":\"parallelize at SparkTC.scala:50\",\"Parent IDs\":[],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":true,\"Deserialized\":true,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":5,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"4\\\",\\\"name\\\":\\\"map\\\"}\",\"Callsite\":\"map at SparkTC.scala:70\",\"Parent IDs\":[4],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":3,\"Name\":\"MapPartitionsRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[2],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0},{\"RDD ID\":2,\"Name\":\"CoGroupedRDD\",\"Scope\":\"{\\\"id\\\":\\\"3\\\",\\\"name\\\":\\\"join\\\"}\",\"Callsite\":\"join at SparkTC.scala:70\",\"Parent IDs\":[0,1],\"Storage Level\":{\"Use Disk\":false,\"Use Memory\":false,\"Deserialized\":false,\"Replication\":1},\"Number of Partitions\":1,\"Number of Cached Partitions\":0,\"Memory Size\":0,\"Disk Size\":0}],\"Parent IDs\":[12,13],\"Details\":\"org.apache.spark.rdd.RDD.distinct(RDD.scala:427)\\norg.apache.spark.examples.SparkTC$.main(SparkTC.scala:70)\\norg.apache.spark.examples.SparkTC.main(SparkTC.scala)\\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\\njava.lang.reflect.Method.invoke(Method.java:498)\\norg.apache.spark.deploy.yarn.ApplicationMaster$$anon$2.run(ApplicationMaster.scala:685)\",\"Accumulables\":[]}],\"Stage IDs\":[15,12,16,13,17,18,14],\"Properties\":{}}\n")
                .toJavaObject(JobStartEvent.class);
        List<JobStartEvent> jobList = new ArrayList<>();
        jobList.add(jse0);
        jobList.add(jse1);
        jobList.add(jse2);
        jobList.add(jse3);
        Integer[] ref = {32, 11, 9, 9, 9, 9, 9, 9, 7, 5, 2, 2, 2, 2, 2, 2, 2, 2};
        Integer[] directRef = {12, 5, 3, 3, 3, 3, 3, 5, 5, 5, 2, 2, 2, 2, 2, 2, 2, 2};
        Integer[] hopComputeTime = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
        Float[] partitions = {1f, 1f, 1f, 1f, 1f, 1f, 2f, 2f, 2f, 2f, 2f, 2f, 2f, 2f, 4f, 4f, 4f, 4f};
        Float[] ce = new Float[18], logCe = new Float[18], directCe = new Float[18], logDirectCe = new Float[18];
        Long[] longPartitions = {1L, 1L, 1L, 1L, 1L, 1L, 2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L, 4L, 4L, 4L, 4L};
        Object[][] contrast = {ref, directRef, hopComputeTime, longPartitions, ce, logCe, directCe, logDirectCe};

        {
            // ResultOutputer.writeSketchStatistics(jobList, fileName);
            String fileName = "test_sketch_statistics";
            for(int i = 0; i < 18; i++) {
                float log = (float) Math.log(hopComputeTime[i] / partitions[i]);
                ce[i] = (ref[i] - 1) * hopComputeTime[i] / partitions[i];
                logCe[i] = Math.max(0, (ref[i] - 1) * log);
                directCe[i] = (directRef[i] - 1) * hopComputeTime[i] / partitions[i];
                logDirectCe[i] = Math.max(0, (directRef[i] - 1) * log);
            }
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            for(Object[] arr : contrast) {
                StringBuilder sb = new StringBuilder();
                assertNotEquals(0, arr.length);
                for(int i = 0; i < arr.length - 1; i++) {
                    sb.append(arr[i].toString()).append(' ');
                }
                sb.append(arr[arr.length - 1].toString());
                System.out.println("asserting: " + sb.toString());
                assertEquals(sb.toString(), br.readLine());
            }
            br.close();
        }
        {
            String fileName = "test_sketch_statistics_plus_one";
            ResultOutputer.writeFullSketchStatistics(jobList, fileName, null);
            for(int i = 0; i < 18; i++) {
                float log = (float) Math.log(hopComputeTime[i] / partitions[i] + 1);
                ce[i] = (ref[i] - 1) * hopComputeTime[i] / partitions[i];
                logCe[i] = (ref[i] - 1) * log;
                directCe[i] = (directRef[i] - 1) * hopComputeTime[i] / partitions[i];
                logDirectCe[i] = (directRef[i] - 1) * log;
            }
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            for(Object[] arr : contrast) {
                StringBuilder sb = new StringBuilder();
                assertNotEquals(0, arr.length);
                for(int i = 0; i < arr.length - 1; i++) {
                    sb.append(arr[i].toString()).append(' ');
                }
                sb.append(arr[arr.length - 1].toString());
                System.out.println("asserting log : " + sb.toString());
                assertEquals(sb.toString(), br.readLine());
            }
            br.close();
        }
    }

    @Test
    void testGenerateApplicationFile() throws Exception {
        {
            String fileName = "E:\\Google Chrome Download\\";
            String[] applicationName = StaticSketch.applicationName;
            String[] applicationPath = StaticSketch.applicationPath;
            for(int i = 0; i < applicationName.length; i++) {
                System.out.println("asserting generate statistics of application: " + applicationName[i]);
                if(new File(applicationName[i] + "_sketch_statistics_new").exists()) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                ResultOutputer.writeSketchStatistics(jobList, applicationName[i] + "_sketch_statistics_new", stageList);
            }
        }
    }

    @Test
    void testGenerateSimpleDAGAndHitStage() throws IOException {
        {
            String fileName = "E:\\Google Chrome Download\\";
            String[] applicationName = StaticSketch.applicationName;
            String[] applicationPath = StaticSketch.applicationPath;
            for(int i = 0; i < applicationName.length; i++) {//applicationName.length
                System.out.println("test application's : " + applicationName[i] + " simple DAG");
                if(new File(applicationName[i] + "_stage_hit_info_new").exists()) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                ResultOutputer.writeStageHitInfo(jobList, stageList, applicationName[i] + "_stage_hit_info_new");
            }
        }
    }

    @Test
    void testGenerateSimpleDAGUI() throws IOException {
        {
            String fileName = "E:\\Google Chrome Download\\";
            String outputDir = "C:\\Users\\小小冰\\Desktop\\MasterLearning\\research\\毕业论文\\test_graph_viz\\";
            // CC, SVM, TeraSort
//            String[] applicationName = {sketch.StaticSketch.applicationName[0], sketch.StaticSketch.applicationName[5], sketch.StaticSketch.applicationName[16]};
//            String[] applicationPath = {sketch.StaticSketch.applicationPath[0], sketch.StaticSketch.applicationPath[5], sketch.StaticSketch.applicationPath[16]};
            String[] applicationName = StaticSketch.applicationName;
            String[] applicationPath = StaticSketch.applicationPath;
            for(int i = 0; i < applicationName.length; i++) {//applicationName.length
                System.out.println("test application's : " + applicationName[i] + " simple DAG UI with .gv");
                if(new File(outputDir + applicationName[i] + ".gv").exists()) { //_DAG_UI
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                ResultOutputer.writeSimpleDAGUI(jobList, stageList, outputDir + applicationName[i] + ".gv");
            }
        }
    }

    @Test
    void testKeyPath() throws IOException {
        String fileName = "E:\\Google Chrome Download\\";
        String[] applicationName = StaticSketch.applicationName;
        String[] applicationPath = StaticSketch.applicationPath;
        {
            for(int j = 0; j < applicationName.length; j++) {//applicationName.length
                System.out.println("test application's : " + applicationName[j] + " key_path");
                if(!applicationName[j].contains("spark_svm")) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[j]);
                for(JobStartEvent job : jobList) {
                    if(job.jobId == 6L) {
                        long[] chose = {0L, 1L, 2L, 3L};
                        List<Long> choseRDD = new ArrayList<>();
                        for(long l : chose) {
                            choseRDD.add(l);
                        }
                        ResultOutputer.writeCacheToTime(choseRDD, job, applicationName[j], applicationName[j] + "_job_" +
                                job.jobId + "_key_path.csv", stageList);
                    }
                }
            }
        }
        {
            for(int j = 0; j < applicationName.length; j++) {//applicationName.length
                System.out.println("test application's : " + applicationName[j] + " key_path");
                if(!applicationName[j].contains("spark_svm")) {
                    continue;
                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[j]);
                for(JobStartEvent job : jobList) {
                    if(job.jobId == 6L) {
                        long[] chose = {2L, 3L, 27L};
                        List<Long> choseRDD = new ArrayList<>();
                        for(long l : chose) {
                            choseRDD.add(l);
                        }
                        ResultOutputer.writeCacheToTime(choseRDD, job, applicationName[j], applicationName[j] + "_job_" +
                                job.jobId + "_key_path_another.csv", stageList);
                    }
                }
            }
        }
    }

    // KEYPOINT
    @Test
    void testKeyPathForAllApplication() throws IOException {
//        if(new File("key_path_cdf_sum.csv").exists() || new File("key_path_cdf_detail.csv").exists()) {
//            return;
//        }
        String fileName = "E:\\Google Chrome Download\\";
        String[] applicationName = StaticSketch.applicationName;
        String[] applicationPath = StaticSketch.applicationPath;
        {
            for(int j = 0; j < applicationName.length; j++) {//applicationName.length
                System.out.println("test application's : " + applicationName[j] + " key path");
//                if(new File(applicationName[j] + "_jobs_key_path.csv").exists()) {
//                    System.out.println("already exists!!!");
//                    continue;
//                } // TODO: need to recover
//                if(!applicationName[j].contains("spark_svm")) {
//                    continue;
//                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[j]);
                List<Long> toCacheInApplication = SimpleUtil.rddToCacheInApplication(jobList, stageList);
                for(JobStartEvent job : jobList) {
                    if(SimpleUtil.jobContainsParallelStages(job, stageList)) {
                        List<Long> toCacheInJob = SimpleUtil.rddToCacheInJob(toCacheInApplication, job, stageList);
                        if(toCacheInJob.size() > 21) {
                            System.out.println("skip job_" + job.jobId + ", for 2^" + toCacheInJob.size());
                            continue;
                        }
                        long startTime = System.currentTimeMillis();
                        System.out.println("write " + applicationName[j] + ", job: " + job.jobId + " cache to time started!!! \n " +
                                "estimated time: " + "2^" + toCacheInJob.size());
                        ResultOutputer.writeCacheToTime(toCacheInJob, job, applicationName[j], "_jobs_key_path.csv", stageList);
                        long endTime = System.currentTimeMillis();
                        System.out.println("write " + applicationName[j] + ", job: " + job.jobId + " cache to time done!!!");
                        System.out.println("actual time: " + (endTime - startTime)+ "ms");
                    }
                }
                System.out.println("write " + applicationName[j] + " cache to time done!!!");
            }
        }
    }

    @Test
    void testStageAllRDDPartition() throws IOException {
        String fileName = "E:\\Google Chrome Download\\";
        String[] applicationName = StaticSketch.applicationName;
        String[] applicationPath = StaticSketch.applicationPath;
        {
//            if(new File("stage_all_rdd_partition.csv").exists()) {
//                return;
//            }
            BufferedWriter bw = new BufferedWriter(new FileWriter("stage_all_rdd_partition.csv"));
            for(int j = 0; j < applicationName.length; j++) {
//                if(!applicationName[j].contains("spark_svm")) {
//                    continue;
//                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[j]);
                Set<Long> actualStage = new HashSet<>();
                for(StageCompletedEvent sce : stageList) {
                    actualStage.add(sce.stage.stageId);
                }
                int curDealStageSize = 0;
                for(JobStartEvent job : jobList) {
                    for(Stage stage : job.stages) {
                        if (!actualStage.contains(stage.stageId)) {
                            continue;
                        }
                        ResultOutputer.stageAllRDDPartition(applicationName[j], job, stage, bw);
                        System.out.println("process: " + ++curDealStageSize + "/" + stageList.size());
                    }
                }
            }
            bw.close();
        }
    }

    @Test
    void testStageCachedRDDPartition() throws IOException {
        String fileName = "E:\\Google Chrome Download\\";
        String[] applicationName = StaticSketch.applicationName;
        String[] applicationPath = StaticSketch.applicationPath;
        {
//            if(new File("stage_cached_rdd_partition.csv").exists()) {
//                return;
//            }
            BufferedWriter bw = new BufferedWriter(new FileWriter("stage_cached_rdd_partition.csv"));
            for(int j = 0; j < applicationName.length; j++) {
//                if(!applicationName[j].contains("spark_svm")) {
//                    continue;
//                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[j]);
                int[][] simpleDAG = CacheSketcher.generateSimpleDAGByJobsAndStages(jobList, stageList);
                Set<Long> actualStage = new HashSet<>();
                for(StageCompletedEvent sce : stageList) {
                    actualStage.add(sce.stage.stageId);
                }
                int curDealStageSize = 0;
                for(JobStartEvent job : jobList) {
                    for(Stage stage : job.stages) {
                        if (!actualStage.contains(stage.stageId)) {
                            continue;
                        }
                        ResultOutputer.stageCachedRDDPartition(applicationName[j], job, stage, simpleDAG, jobList.size(), bw);
                        System.out.println("process: " + ++curDealStageSize + "/" + stageList.size());
                    }
                }
            }
            bw.close();
        }
    }

    @Test
    void testWriteTotalAndRepresentTime() throws IOException {
        String fileName = "E:\\Google Chrome Download\\";
        String[] applicationName = StaticSketch.applicationName;
        String[] applicationPath = StaticSketch.applicationPath;
        {
            for (int i = 0; i < applicationName.length; i++) {
//                if (!applicationName[i].contains("spark_svm")) {
//                    continue;
//                }
//                if(applicationName[i].contains("spark_strongly")) {
//                    continue;
//                }
                List<JobStartEvent> jobList = new ArrayList<>();
                List<StageCompletedEvent> stageList = new ArrayList<>();
                StaticSketch.generateJobAndStageList(jobList, stageList, fileName + applicationPath[i]);
                List<Long> allToCache = SimpleUtil.rddToCacheInApplication(jobList, stageList);
                // index from 0 to len-1
                Set<Long> actualStage = new HashSet<>();
                for(StageCompletedEvent sce : stageList) {
                    actualStage.add(sce.stage.stageId);
                }
                int curJob = 0;
//                for(int j = 0; j < jobList.size(); j++) { // KEYPONT 先算简单的
                for(int j = jobList.size() - 1; j >= 0; j--) {
                    // [j, jobList.size() - 1]
                    System.out.println(applicationName[i] + ": " + ++curJob + "/" + jobList.size());
                    List<JobStartEvent> newJobList = jobList.subList(j, jobList.size());
                    List<Stage> newStageList = new ArrayList<>();
                    List<Long> rddToCache = new ArrayList<>();
                    for (int k = j; k < jobList.size(); k++) {
                        for (Stage stage : jobList.get(k).stages) {
                            if (actualStage.contains(stage.stageId)) {
                                newStageList.add(stage);
                                for (RDD rdd : stage.rdds) {
                                    if (allToCache.contains(rdd.rddId)) {
                                        if(!rddToCache.contains(rdd.rddId)) {
                                            rddToCache.add(rdd.rddId);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    System.out.println(rddToCache);
                    System.out.println("estimated time: 2^" + rddToCache.size() + " job size: " + (jobList.size() - j));
                    if(rddToCache.size() > 21) {
                        System.out.println("skip start job_" + jobList.get(j).jobId + ", for 2^" + rddToCache.size());
                        continue;
                    }
                    long startTime = System.currentTimeMillis();
                    ResultOutputer.writeTotalAndRepresentTime(newJobList, newStageList, rddToCache, applicationName[i]);
                    long endTime = System.currentTimeMillis();
                    System.out.println("actual time: " + (endTime - startTime) + "ms(2^" + rddToCache.size() + ", job size: " + (jobList.size() - j) + ")");
                }
            }
        }
    }

}
