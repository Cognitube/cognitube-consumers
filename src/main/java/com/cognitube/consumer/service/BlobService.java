package com.cognitube.consumer.service;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.cognitube.consumer.service.exception.CloudStorageException;
import com.cognitube.consumer.enums.ContainerName;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * @program: Cognitube
 * @description: the implementation of blob storage
 * @author: Yaowen Hu
 * @create: 2024-02-08 20:50
 **/
@Service
@AllArgsConstructor
public class BlobService {

    private static final Logger logger = LoggerFactory.getLogger(BlobService.class);
    private final BlobServiceClient blobServiceClient;
    private final String videoContainerName;
    private final String imageContainerName;
    private final String keywordsContainerName;
    private final String profileImageContainerName;
    private final String audioContainerName;
    private final String tempVideoContainerName;
    private final String azureFrontdoorUrl;

    public String uploadVideoGetRelativeUrl(File video) {
        String fullUrl = uploadFileToBlob(video, videoContainerName);
        return extractRelativePathFromBlobUrl(fullUrl);
    }

    public String uploadTempVideoGetFullUrl(File video) {
        return uploadFileToBlob(video, tempVideoContainerName);
    }

    public String uploadAudio(File audio) {
        return uploadFileToBlob(audio, audioContainerName);
    }

    public String uploadImage(File image, ContainerName containerName) {
        String container = switch (containerName) {
            case PROFILE_IMAGE -> profileImageContainerName;
            case IMAGE -> imageContainerName;
        };
        String fullUrl = uploadFileToBlob(image, container);
        return extractRelativePathFromBlobUrl(fullUrl);
    }

    /**
     * Extract the relative path of the blob from the full URL
     * @param blobUrl
     * @return
     */
    private String extractRelativePathFromBlobUrl(String blobUrl) {
        // Azure Blob Storage的基础URL格式通常是 https://<account-name>.blob.core.windows.net/
        // 这里我们需要从完整的Blob URL中移除基础URL 只留下文件路径和容器名
        String baseUrlPattern = "^https://[^/]+\\.blob\\.core\\.windows\\.net/";
        String fullPath = blobUrl.replaceFirst(baseUrlPattern, "");
        return fullPath;
    }

    public String uploadKeywords(String contentString, String videoName) {
        byte[] byteArray = contentString.getBytes(StandardCharsets.UTF_8);
        try (InputStream dataStream = new ByteArrayInputStream(byteArray)) {
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(keywordsContainerName);
            logger.info("the blobContainerClient initialized successfully, the containerName is " + keywordsContainerName);
            String fileName = System.currentTimeMillis() + "-jsonArray";
            BlobClient blobClient = containerClient.getBlobClient(fileName);
            logger.info("start to upload keywords to blob storage, the video name is " + videoName);
            blobClient.upload(dataStream, byteArray.length, true);
            logger.info("upload keywords json array successfully! the video name is " + videoName);
            return blobClient.getBlobUrl();
        } catch (Exception e) {
            logger.error("upload keywords failed, please check the api.");
            throw new CloudStorageException("Failed to upload keywords to Azure Blob Storage", e);
        }
    }

    private String uploadFileToBlob(File file, String containerName) {
        try {
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            logger.info("the blobContainerClient initialized successfully, the containerName is " + containerName);
            String fileName = System.currentTimeMillis() + "-" + file.getName();
            logger.info("the file will be uploaded to blob " + containerName + " is " + fileName);
            BlobClient blobClient = containerClient.getBlobClient(fileName);
            FileInputStream fileInputStream = new FileInputStream(file);
            blobClient.upload(fileInputStream, file.length(), true);
            fileInputStream.close();
            return blobClient.getBlobUrl();
        } catch (Exception e) {
            logger.error("upload failed, please check the format of the file!");
            throw new CloudStorageException("Failed to upload file to Azure Blob Storage", e);
        }
    }

    /**
     * Get the full path of the resource in CDN given relative path of the resource
     * @param relativePath
     * @param containerName
     * @return
     */
    public String getCDNFullPath(String relativePath, String containerName) {
        return azureFrontdoorUrl + "/" + relativePath;
    }

    public File downloadFile(String blobUrl) {
        try {
            URI uri = new URI(blobUrl);
            String path = uri.getPath();
            String containerName = path.split("/")[1];
            String blobName = path.substring(containerName.length() + 2);

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);

            File tempFile = File.createTempFile("download-", ".tmp");
            try (FileOutputStream outputStream = new FileOutputStream(tempFile)) {
                blobClient.download(outputStream);
            }
            logger.info("Downloaded file to " + tempFile.getAbsolutePath());
            return tempFile;
        } catch (Exception e) {
            logger.error("Download failed, please check the blob URL!", e);
            throw new CloudStorageException("Failed to download file from Azure Blob Storage", e);
        }
    }

    public void deleteFile(String blobUrl) {
        try {
            URI uri = new URI(blobUrl);
            String path = uri.getPath();
            String containerName = path.split("/")[1];
            String blobName = path.substring(containerName.length() + 2);

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.delete();
            logger.info("Deleted file " + blobName + " from container " + containerName);
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                logger.warn("File " + blobUrl + " does not exist.");
            } else {
                logger.error("Delete failed, please check the blob URL!", e);
                throw new CloudStorageException("Failed to delete file from Azure Blob Storage", e);
            }
        } catch (Exception e) {
            logger.error("Delete failed, please check the blob URL!", e);
            throw new CloudStorageException("Failed to delete file from Azure Blob Storage", e);
        }
    }

}
