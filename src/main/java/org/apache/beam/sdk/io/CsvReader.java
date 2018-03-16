package org.apache.beam.sdk.io;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class CsvReader extends FileBasedSource.FileBasedReader<CSVRecord> {

    private CSVParser csvParser;
    private Map<String, Integer> header;
    private Iterator<CSVRecord> csvRecordIterator;
    private long recordsNumber = 0;

    /**
     * Subclasses should not perform IO operations at the constructor. All IO operations should be
     * delayed until the {@link #startReading} method is invoked.
     *
     * @param source
     */
    public CsvReader(FileBasedSource<CSVRecord> source) {
        super(source);
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
        this.csvParser = CSVFormat.newFormat(',')
                .withNullString("null")
                .withSkipHeaderRecord(false)
                .withHeader(new String[]{})
                .withQuote('"')
                .parse(new ChannelReader(channel));
        this.header = csvParser.getHeaderMap();
        this.csvRecordIterator = this.csvParser.iterator();
    }

    @Override
    protected boolean readNextRecord() throws IOException {
        return this.csvRecordIterator.hasNext();
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
        return this.recordsNumber;
    }

    @Override
    public CSVRecord getCurrent() throws NoSuchElementException {
        return this.csvRecordIterator.next();
    }
}
