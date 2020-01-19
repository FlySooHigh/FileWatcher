package org.flysoohigh.fileWatcher.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class StreamHandler extends Thread {
    InputStream is;
    String streamType;

    public StreamHandler(InputStream is, String streamType) {
        this.is = is;
        this.streamType = streamType;
    }

    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null)
                System.out.println(streamType + "> " + line);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
