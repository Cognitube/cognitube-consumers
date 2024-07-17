package com.cognitube.consumer.service;

import com.cognitube.consumer.service.exception.NoAudioTrackException;
import com.cognitube.consumer.util.Constants;
import com.cognitube.consumer.util.FileNameGenerator;
import com.cognitube.consumer.util.StreamGobbler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author Haozhe Zhang
 * @version 1.0
 * @project cognitube-consumers
 * @description FFMPEG ops
 * @date 2024/7/17 15:56:27
 */
@Service
@Slf4j
public class VideoEncodingService {

    @Value("${application.audio.max.size}")
    private long MAX_FILE_SIZE_BYTES;

    /**
     * Re-encode a video file to a standard format
     * @param source
     * @param target
     */
    public void reencodeVideo(File source, File target) {
        // Build the ffmpeg command with detailed comments for each option
        List<String> command = Arrays.asList(
                "ffmpeg",
                "-y", // -y: Overwrite output files without asking
                "-i", source.getAbsolutePath(), // -i: Input file path
                "-c:v", "libx264", // -c:v: Video codec, use H.264 codec
                "-b:v", "800k", // -b:v: Video bitrate, set video bitrate to 800 kbps
                "-r", "30", // -r: Frame rate, set frame rate to 30 fps
                "-c:a", "aac", // -c:a: Audio codec, use AAC codec
                "-b:a", "128k", // -b:a: Audio bitrate, set audio bitrate to 128 kbps
                "-ac", "2", // -ac: Audio channels, set number of audio channels to 2 (stereo)
                "-ar", "44100", // -ar: Audio sampling rate, set audio sampling rate to 44100 Hz
                "-threads", "0", // -threads: Automatically determine the number of threads to use
                target.getAbsolutePath() // Output file path
        );

        log.info("command: " + String.join(" ", command));
        // Use ProcessBuilder to execute the ffmpeg command
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true); // Redirect error stream to standard output

        try {
            Process process = builder.start();
            // Read and print the output from the command
            StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT", false);
            StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR", false);
            outputGobbler.start();
            errorGobbler.start();

            int exitCode = process.waitFor(); // Wait for the process to complete
            outputGobbler.join();
            errorGobbler.join();
            if (exitCode != 0) {
                throw new RuntimeException("ffmpeg exited with error code " + exitCode);
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute ffmpeg command", e);
        }
    }

    /**
     * Extract audio from a video file
     * @param videoFile
     * @return
     * @throws NoAudioTrackException
     */
    public File convertVideoToAudio(File videoFile) {
        boolean videoHasAudio = hasAudio(videoFile);
        if (!videoHasAudio) {
            throw new NoAudioTrackException("The video does not have an audio track");
        }

        // Specify the output audio file
        File audioFile = new File(videoFile.getParent(), FileNameGenerator.generateUniqueFileName("audio", "oga"));

        // ffmpeg: Invokes the ffmpeg command-line tool. It is used here for audio processing.
        // -i: Specifies the input file. Here, it indicates the video file from which the audio will be extracted.
        //     The placeholder %s will be replaced by `videoFile.getAbsolutePath()`, which provides the path to the video file.
        List<String> command = Arrays.asList(
                "ffmpeg",
                "-i", videoFile.getAbsolutePath(), // -i: Specifies the input file.
                "-y", // -y: Overwrite output files without asking.

                // -acodec libopus: Specifies the audio codec. 'libopus' is chosen for its efficiency and high quality, particularly suitable for voice and music in low to mid-bitrate ranges.
                // -b:a: Sets the audio bitrate. Here it is set using 'Constants.DEFAULT_AUDIO_BIT_RATE_KBPS' which is likely defined elsewhere in the code.
                //       This bitrate, followed by 'k', indicates kilobits per second, ensuring that the audio is encoded at this specified rate.
                "-acodec", "libopus", "-b:a", Constants.DEFAULT_AUDIO_BIT_RATE_KBPS + "k",

                // -ac 1: Sets the number of audio channels. '1' means mono audio output, which is typically used for voice recordings to reduce file size and complexity.
                // -ar: Sets the audio sampling rate. Here, it uses 'Constants.DEFAULT_SAMPLING_RATE', defining how many samples per second are captured in the audio file.
                "-ac", "1", "-ar", String.valueOf(Constants.DEFAULT_SAMPLING_RATE),

                // %s: This placeholder will be replaced by `audioFile.getAbsolutePath()`, which indicates the path where the processed audio file will be saved.
                //      This part of the command specifies the output file location and name.
                audioFile.getAbsolutePath()
        );

        log.error("command: " + String.join(" ", command));
        // 使用 ProcessBuilder 执行命令
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);  // 将错误输出重定向到标准输出
        Process process = null;
        try {
            process = builder.start();

            // Consume the process output and error streams to prevent blocking
            StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT", false);
            StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR", false);
            outputGobbler.start();
            errorGobbler.start();

            int exitCode = process.waitFor();
            outputGobbler.join();
            errorGobbler.join();

            if (exitCode != 0) {
                throw new RuntimeException("Audio extraction failed with exit code " + process.exitValue());
            }
        } catch (IOException | InterruptedException e) {
            if (process != null) process.destroy();
            throw new RuntimeException("Error executing ffmpeg command to process audio", e);
        } finally {
            if (process != null) process.destroy();
        }

        return audioFile;
    }

    /**
     * Check if the video file has an audio track
     * @param videoFile
     * @return
     */
    private static boolean hasAudio(File videoFile) {
        try {
            // Build the ffmpeg command to get media file information
            List<String> command = Arrays.asList(
                    "ffmpeg",
                    "-i", videoFile.getAbsolutePath(),  // -i: Specifies the input file.
                    "-hide_banner",                    // -hide_banner: Hides the startup banner.
                    "-f", "null", "-"                  // -f null: Use null muxer to discard output.
            );

            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectErrorStream(true); // Redirect error stream to standard output
            Process process = builder.start();
            boolean hasAudio = false;

            // Read and analyze the output from ffmpeg to determine if there is an audio stream
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.contains("Audio:")) { // Check if the line contains information about an audio stream
                        hasAudio = true;
                        break; // Break out of the loop once an audio track is found
                    }
                }
            }

            int exitCode = process.waitFor(); // Wait for the process to complete

            if (exitCode != 0) {
                throw new RuntimeException("ffmpeg process exited with error code " + exitCode);
            }
            return hasAudio;

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute ffmpeg command", e);
        }
    }
}
