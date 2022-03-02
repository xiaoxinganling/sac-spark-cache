import entity.RDD;
import entity.Stage;
import entity.event.JobStartEvent;
import entity.event.StageCompletedEvent;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;
import sketch.StaticSketch;
import sun.rmi.runtime.Log;
import utils.CacheSketcher;
import utils.ResultOutputer;
import utils.SimpleUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class TestPicDownload {


    String[] applicationName = StaticSketch.applicationName;
    String[] applicationPath = StaticSketch.applicationPath;

    @Test
    void testDownload () {
        for (int i = 0; i < applicationName.length; i++) {
            System.out.println(String.format("http://114.212.82.49:18080/history/%s/jobs/", applicationPath[i])
//            );
                    + " "
            + String.format("G:\\dislab的资料\\web_xft\\%s", String.format("%03d_", i + 1) + applicationName[i]));
        }
    }

}
