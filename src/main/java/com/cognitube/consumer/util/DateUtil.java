package com.cognitube.consumer.util;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-backend
 * @description Util class for holding date related methods
 * @date 2024/3/3 12:38:38
 */
public class DateUtil {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private static final DateTimeFormatter ISO_OFFSET_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)  // 添加微秒精度
            .appendOffset("+HH:mm", "")
            .toFormatter();

    /**
     * Format LocalDateTime to string
     * @param dateTime
     * @return
     */
    public static String formatLocalDateTime(LocalDateTime dateTime) {
        return FORMATTER.format(dateTime);
    }

    /**
     * Parse string to LocalDateTime
     * @param dateTime
     * @return
     */
    public static LocalDateTime parseLocalDateTime(String dateTime) {
        if (dateTime == null || dateTime.isEmpty()) {
            return null;
        }

        return LocalDateTime.parse(dateTime, FORMATTER);
    }

    /**
     * Parse string to LocalDateTime in ISO
     * @param dateTime
     * @return
     */
    public static LocalDateTime parseLocalDateTimeISO(String dateTime) {
        if (dateTime == null || dateTime.isEmpty()) {
            return null;
        }

        return LocalDateTime.parse(dateTime, ISO_OFFSET_DATE_TIME_FORMATTER);
    }

    /**
     * Get current timestamp in string
     * @return
     */
    public static String getTimestampStringISO() {
        Instant now = Instant.now();
        ZoneId systemZone = ZoneId.systemDefault();
        OffsetDateTime currentTime = now.atZone(systemZone).toOffsetDateTime();

        return ISO_OFFSET_DATE_TIME_FORMATTER.format(currentTime);
    }

    /**
     * Convert ISO timestamp to epoch seconds
     * This method is used to convert the timestamp from the frontend to epoch seconds
     * Time extracted from Postgresql in timstamp with time zone is in the ISO format
     * @param timestampString
     * @return
     */
    public static long convertISOToEpochSeconds(String timestampString) {
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestampString, ISO_OFFSET_DATE_TIME_FORMATTER);
        long epochSeconds = offsetDateTime.toEpochSecond();

        return epochSeconds;
    }

    /**
     * Convert ISO timestamp to epoch milliseconds
     * This method is used to convert the timestamp from the frontend to epoch seconds
     * Time extracted from Postgresql in timstamp with time zone is in the ISO format
     * @param timestampString
     * @return
     */
    public static long convertISOToEpochMilliseconds(String timestampString) {
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestampString, ISO_OFFSET_DATE_TIME_FORMATTER);
        long epochMilli = offsetDateTime.toInstant().toEpochMilli();

        return epochMilli;
    }

    /**
     * Convert string to epoch milliseconds using the default formatter
     * @param timeStr
     * @return
     */
    public static long convertStringToMillis(String timeStr) {
        LocalDateTime dateTime = LocalDateTime.parse(timeStr, FORMATTER);
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /**
     * Get current date in string
     * @param date
     * @return
     */
    public static String getCurrentDateString(LocalDate date) {
        return date.format(DATE_FORMATTER);
    }

    /**
     * Get current month in string
     * @param date
     * @return
     */
    public static String getCurrentMonthString(LocalDate date) {
        return date.format(MONTH_FORMATTER);
    }

    /**
     * Get current month
     * @param date
     * @return
     */
    public static LocalDate parseLocalMonth(String date) {
        return LocalDate.parse(date, MONTH_FORMATTER);
    }

    /**
     * Get current date
     * @param date
     * @return
     */
    public static LocalDate parseLocalDate(String date) {
        return LocalDate.parse(date, DATE_FORMATTER);
    }

    /**
     * Get the week of year
     * @param date
     * @return
     */
    public static int getWeekOfYear(LocalDate date) {
        return date.get(ChronoField.ALIGNED_WEEK_OF_YEAR);
    }
}
