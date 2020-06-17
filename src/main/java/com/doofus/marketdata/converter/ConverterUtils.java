package com.doofus.marketdata.converter;

import com.doofus.market.BseDataParser;
import com.doofus.market.model.bse.equity.BseEquityInputRecord;
import com.doofus.market.model.bse.equity.BseEquityOutputRecord;
import org.apache.commons.io.FilenameUtils;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.stream.Collectors;

public class ConverterUtils {

  static BseDataParser bseDataParser = new BseDataParser();

  public static List<String> getObjectsToConvert(List<String> allObjectPaths) {
    return allObjectPaths.stream()
        .filter(object -> isCSV(object) && !isConverted(object, allObjectPaths))
        .collect(Collectors.toList());
  }

  public static boolean isCSV(String object) {
    return object.toLowerCase().endsWith(".csv");
  }

  public static boolean isConverted(String object, List<String> allObjectPaths) {
    return allObjectPaths.contains(object.replace(getExtension(object), "txt"));
  }

  public static String getExtension(String filename) {
    return FilenameUtils.getExtension(filename);
  }

  public static byte[] parseBSE(InputStream bseInputStream) {
    final List<BseEquityInputRecord> bseEquityInputRecords = bseDataParser.read(bseInputStream);
    final List<BseEquityOutputRecord> bseEquityOutputRecords =
        bseDataParser.convert(bseEquityInputRecords);
    return bseDataParser.write(bseEquityOutputRecords, new StringWriter());
  }
}
