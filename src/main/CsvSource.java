package com.db.awmd.lambda.csv2;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class CsvSource extends FileBasedSource<CSVRecord> {
    public CsvSource(ValueProvider<String> fileOrPatternSpec) {
        super(fileOrPatternSpec, 1000000L);
    }

    protected CsvSource(MatchResult.Metadata fileMetadata, long startOffset, long endOffset) {
        super(fileMetadata, 1000000L, startOffset, endOffset);
    }

    @Override
    protected FileBasedSource<CSVRecord> createForSubrangeOfFile(MatchResult.Metadata fileMetadata, long start, long end) {
        return new CsvSource(fileMetadata, start, end);
    }

    @Override
    protected FileBasedReader<CSVRecord> createSingleFileReader(PipelineOptions options) {
        return new CsvBasedReader(this);
    }

    @Override
    public Coder<CSVRecord> getDefaultOutputCoder() {
        return CsvCoder.of();
    }

    static class CsvBasedReader extends FileBasedReader<CSVRecord> {

        private CSVParser csvParser;
        private Iterator<CSVRecord> csvRecordIterator;
        private CSVRecord currentCsvRecord;
        private Map<String, Integer> headerMap;

        /**
         * Subclasses should not perform IO operations at the constructor. All IO operations should be
         * delayed until the {@link #startReading} method is invoked.
         *
         * @param source
         */
        public CsvBasedReader(FileBasedSource<CSVRecord> source) {
            super(source);
        }

        @Override
        protected void startReading(ReadableByteChannel channel) throws IOException {
            CsvSource source = (CsvSource) getCurrentSource();
            this.csvParser = new CSVParser(Channels.newReader(channel, "UTF-8"), CSVFormat.DEFAULT);
            this.headerMap = csvParser.getHeaderMap();
            this.csvRecordIterator = this.csvParser.iterator();
        }

        @Override
        protected boolean readNextRecord() throws IOException {
            boolean hasNext = csvRecordIterator.hasNext();
            if (hasNext) {
                this.currentCsvRecord = csvRecordIterator.next();
            }
            return hasNext;
        }

        @Override
        protected long getCurrentOffset() throws NoSuchElementException {
            return csvParser.getCurrentLineNumber();
        }

        @Override
        public CSVRecord getCurrent() throws NoSuchElementException {
            CSVRecord csvRecord = this.currentCsvRecord;
            this.currentCsvRecord = null;
            return csvRecord;
        }
    }
}
