package ua.realtime.twitter.sentimental;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alukard on 4/29/15.
 */
public class DictionaryReader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DictionaryReader.class);

    public static final String COLUMN_WORD = "word";
    public static final String COLUMN_HAPPINESS_RANK = "happiness_rank";
    public static final String COLUMN_HAPPINESS_AVERAGE = "happiness_average";
    public static final String COLUMN_HAPPINESS_STANDARD_DEVIATION = "happiness_standard_deviation";
    public static final String COLUMN_TWITTER_RANK = "twitter_rank";
    public static final String COLUMN_GOOGLE_RANK = "google_rank";
    public static final String COLUMN_NYT_RANK = "nyt_rank";
    public static final String COLUMN_LYRICS_RANK = "lyrics_rank";

    //CSV file header
    private static final String [] FILE_HEADER_MAPPING = {COLUMN_WORD, COLUMN_HAPPINESS_RANK, COLUMN_HAPPINESS_AVERAGE,
            COLUMN_HAPPINESS_STANDARD_DEVIATION, COLUMN_TWITTER_RANK, COLUMN_GOOGLE_RANK, COLUMN_NYT_RANK, COLUMN_LYRICS_RANK};

    private static final long serialVersionUID = -9115155698026246909L;

    public Map<String, Entry> readCsvFile(final String fileName) throws URISyntaxException {

        Map<String, Entry> dictionary = new HashMap<>();

        //Create the CSVFormat object with the header mapping
        CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(FILE_HEADER_MAPPING).withDelimiter('\t').withSkipHeaderRecord();

        try(FileReader fileReader = new FileReader(new File(fileName));
            CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat);) {

            //Get a list of CSV file records
            List<CSVRecord> csvRecords = csvFileParser.getRecords();

            LOG.info("Dictionary file contains words: " + csvRecords.size());

            //Read the CSV file records starting from the second record to skip the header
            for (int i = 1; i < csvRecords.size(); i++) {
                CSVRecord record = csvRecords.get(i);

                Double happinessAverage;

                try {
                    happinessAverage = Double.valueOf(record.get(COLUMN_HAPPINESS_AVERAGE));
                } catch (NumberFormatException e) {
                    continue;
                }

                Entry entry = new Entry(record.get(COLUMN_WORD), record.get(COLUMN_HAPPINESS_RANK),
                        happinessAverage, record.get(COLUMN_HAPPINESS_STANDARD_DEVIATION),
                        record.get(COLUMN_TWITTER_RANK),record.get(COLUMN_GOOGLE_RANK),
                        record.get(COLUMN_NYT_RANK), record.get(COLUMN_LYRICS_RANK));

                dictionary.put(entry.getWord(), entry);
            }
        } catch (Exception e) {
            LOG.error("Error in CsvFileReader !!!", e);
        }

        return dictionary;
    }
}
