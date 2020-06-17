package com.doofus.marketdata.converter;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Arrays;

@SpringBootApplication
@RestController
@RequestMapping("/convert")
public class ConverterApplication {

  @Autowired private Environment environment;

  private static final Logger LOGGER = LoggerFactory.getLogger(ConverterApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(ConverterApplication.class, args);
  }

  @GetMapping("/bse")
  public ResponseEntity<String> convertBse() {

    GoogleCloudStorageHelper googleCloudStorageHelper =
        new GoogleCloudStorageHelper(
            environment.getProperty("PROJECT"), environment.getProperty("BUCKET"));
    String bsePrefix = "bse/" + LocalDateTime.now().getYear();
    ConverterUtils.getObjectsToConvert(googleCloudStorageHelper.listObjects(bsePrefix))
        .forEach(
            objectPath -> {
              LOGGER.info("Converting {}", objectPath);
              String writePath = objectPath.replace(FilenameUtils.getExtension(objectPath), "txt");
              googleCloudStorageHelper.uploadObject(
                  writePath,
                  ConverterUtils.parseBSE(googleCloudStorageHelper.getBlobContentAt(objectPath)));
            });

    return ResponseEntity.ok("Request Actioned");
    // TODO redirect to conversion service
  }
}
