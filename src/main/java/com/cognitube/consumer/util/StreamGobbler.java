package com.cognitube.consumer.util;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description Keeps track of process errors and outputs. Also helps clean the output buffer to prevent freezing
 * @date 2024/6/27 03:17:05
 */
@Slf4j
public class StreamGobbler extends Thread {
    private final InputStream inputStream;
    private final String type;
    private final boolean shouldPrintStream;

    public StreamGobbler(InputStream inputStream, String type, boolean shouldPrintStream) {
        this.inputStream = inputStream;
        this.type = type;
        this.shouldPrintStream = shouldPrintStream;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null && shouldPrintStream) {
                if ("ERROR".equals(type)) {
                    log.error(type + "> " + line);
                } else {
                    log.info(type + "> " + line);
                }
            }
        } catch (IOException e) {
            log.error("Error processing stream for " + type, e);
        } catch (Throwable t) {
            log.error("Serious error in stream gobbler for " + type, t);
            // Re-interrupt to maintain thread interruption status
            Thread.currentThread().interrupt();
        }
    }
}