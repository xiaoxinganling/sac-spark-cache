package utils;

import org.junit.jupiter.api.Test;
import simulator.SimulatorProcess;

import java.io.*;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class TestNumberUtil {

    @Test
    void numberWithRatio() throws IOException {
        ArrayList<Integer> memorySizes = new ArrayList<>();
        ArrayList<Integer> parallelisms = new ArrayList<>();
        BufferedReader memBr = new BufferedReader(new FileReader(SimulatorProcess.MEMORY_SIZE_INFO));
        BufferedReader parallelBr = new BufferedReader(new FileReader(SimulatorProcess.PARALLEL_INFO));
        for (String s : memBr.readLine().split("\\s+")) {
            memorySizes.add(Integer.parseInt(s));
        }
        for (String s : parallelBr.readLine().split("\\s+")) {
            parallelisms.add(Integer.parseInt(s));
        }
        System.out.println(memorySizes + " " + parallelisms);
        double[] memRatio = {0.25, 0.5, 0.75};
        double[] parallelismRatio = {0.5, 1};
        for (int i : memorySizes) {
            for (double j : memRatio) {
                System.out.println(i + "*" + j + "= " + i * j + "->" + NumberUtil.numberWithRatio(i, j));
            }
        }
        for (int i : parallelisms) {
            for (double j : parallelismRatio) {
                System.out.println(i + "*" + j + "= " + i * j + "->" + NumberUtil.numberWithRatio(i, j));
            }
        }
    }
}