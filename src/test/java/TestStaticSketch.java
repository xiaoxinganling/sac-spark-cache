import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import entity.StorageLevel;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
public class TestStaticSketch {

    @Test
    void testPCAApplication() throws IOException {
        {
            String fileName = "E:\\Google Chrome Download\\";
            String[] applicationName = StaticSketch.applicationName;
            String[] applicationPath = StaticSketch.applicationPath;
            Set<String> filteredEvent = new HashSet<>();
            int cached = 0, notCached = 0;
            int cachedSize = 0, notCachedSize = 0;
            for(int i = 0; i < applicationName.length; i++) {
                if(applicationName[i].equals("spark_pca")) {
                    BufferedReader br = new BufferedReader(new FileReader(fileName + applicationPath[i]));
                    String line;
                    while((line = br.readLine()) != null) {
                        if(line.contains("\"Memory Size\":")) {
                            if(line.charAt(line.indexOf("\"Memory Size\":") + "\"Memory Size\":".length()) > '0') {
                                JSONObject jsonObject = JSONObject.parseObject(line);
                                filteredEvent.add(jsonObject.get("Event").toString());
                                StorageLevel sl = JSON.parseObject(JSONObject.parseObject(jsonObject.get("Block Updated Info").toString()).get("Storage Level").toString(),
                                        StorageLevel.class);
                                int start = line.indexOf("\"Memory Size\":") + "\"Memory Size\":".length();
                                int end = line.indexOf("\"Disk Size\":") - 1;
                                int size = Integer.parseInt(line.substring(start, end));
                                if(sl.useMemory) {
                                    cached++;
                                    System.out.println("cached: " + line.substring(start, end));
                                    cachedSize += size;
                                    System.out.println(line);
                                }else{
                                    notCached++;
                                    System.out.println("not cached: " + line.substring(start, end));
                                    notCachedSize += size;
//                                System.out.println(line);
                                }
                            }
                        }
                    }
                    br.close();
                }
            }
            System.out.println(filteredEvent);
            System.out.println("not cached: " + notCached + ", cached: " + cached);
            System.out.println("not cached size: " + notCachedSize + ", cached size: " + cachedSize);
        }
        {
            String fileName = "E:\\Google Chrome Download\\";
            String[] applicationName = StaticSketch.applicationName;
            String[] applicationPath = StaticSketch.applicationPath;
            Set<String> filteredEvent = new HashSet<>();
            for(int i = 0; i < applicationName.length; i++) {
                if(applicationName[i].equals("spark_pca")) {
                    BufferedReader br = new BufferedReader(new FileReader(fileName + applicationPath[i]));
                    String line;
                    while((line = br.readLine()) != null) {
                        if(line.contains("Size")) {
//                            if(line.charAt(line.indexOf("\"Memory Size\":") + "\"Memory Size\":".length()) > '0') {
                                JSONObject jsonObject = JSONObject.parseObject(line);
                                filteredEvent.add(jsonObject.get("Event").toString());
//                            }
                        }
                    }
                    br.close();
                }
            }
            System.out.println(filteredEvent);
        }

    }

}
