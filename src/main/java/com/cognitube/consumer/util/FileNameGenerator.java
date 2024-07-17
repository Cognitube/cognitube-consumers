package com.cognitube.consumer.util;

import java.util.UUID;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description Provides a util function to generate a random file name with prefix and suffix
 * @date 2024/6/26 03:32:37
 */
public class FileNameGenerator {
    public static String generateUniqueFileName(String prefix, String suffix) {
        return String.format("%s_%s.%s", prefix, UUID.randomUUID(), suffix);
    }
}
