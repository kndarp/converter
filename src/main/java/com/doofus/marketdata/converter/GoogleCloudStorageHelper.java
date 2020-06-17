package com.doofus.marketdata.converter;

import com.google.cloud.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class GoogleCloudStorageHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudStorageHelper.class);

  private String bucketName;

  private String project;

  public GoogleCloudStorageHelper(String project, String bucketName) {
    this.project = project;
    this.bucketName = bucketName;
  }

  public List<String> listObjects(String directoryPrefix) {
    Storage storage = this.getService();
    Bucket bucket = storage.get(bucketName);

    List<String> objectPaths = new ArrayList<>();
    for (Blob blob : bucket.list(Storage.BlobListOption.prefix(directoryPrefix)).iterateAll()) {
      objectPaths.add(blob.getName());
    }

    return objectPaths;
  }

  private Storage getService() {
    return StorageOptions.newBuilder().setProjectId(this.project).build().getService();
  }

  public InputStream getBlobContentAt(String blobPath) {
    return new ByteArrayInputStream(
        getService().get(BlobId.of(this.bucketName, blobPath)).getContent());
  }

  public void uploadObject(String objectName, byte[] inputStream) {
    Storage storage = this.getService();
    BlobId blobId = BlobId.of(this.bucketName, objectName);
    Blob blob = storage.get(blobId);

    if (blob == null || !blob.exists()) {
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      storage.create(blobInfo, inputStream);
      LOGGER.info("{} Uploaded", objectName);
    } else {
      LOGGER.warn("File already exists. Not Uploading it");
    }
  }
}
