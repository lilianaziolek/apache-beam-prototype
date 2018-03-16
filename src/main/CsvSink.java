package com.db.awmd.lambda.csv2;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

public class CsvSink extends FileBasedSink<CSVRecord> {
    @Nullable
    private final CSVRecord header;
    @Nullable private final CSVRecord footer;

    CsvSink(
            ValueProvider<ResourceId> baseOutputFilename,
            FilenamePolicy filenamePolicy,
            @Nullable CSVRecord header,
            @Nullable CSVRecord footer,
            WritableByteChannelFactory writableByteChannelFactory) {
        super(baseOutputFilename, filenamePolicy, writableByteChannelFactory);
        this.header = header;
        this.footer = footer;
    }
    @Override
    public WriteOperation<CSVRecord> createWriteOperation() {
        return new CsvSink.CsvWriteOperation(this, header, footer);
    }

    /** A {@link WriteOperation WriteOperation} for text files. */
    private static class CsvWriteOperation extends WriteOperation<CSVRecord> {
        @Nullable private final CSVRecord header;
        @Nullable private final CSVRecord footer;

        private CsvWriteOperation(CsvSink sink, @Nullable CSVRecord header, @Nullable CSVRecord footer) {
            super(sink);
            this.header = header;
            this.footer = footer;
        }

        @Override
        public Writer<CSVRecord> createWriter() throws Exception {
            return new CsvSink.CsvWriter(this, header, footer);
        }
    }

    /** A {@link Writer Writer} for text files. */
    private static class CsvWriter extends Writer<CSVRecord> {
        private static final String NEWLINE = "\n";
        @Nullable private final CSVRecord header;
        @Nullable private final CSVRecord footer;
        private OutputStreamWriter out;

        public CsvWriter(
                WriteOperation<CSVRecord> writeOperation,
                @Nullable CSVRecord header,
                @Nullable CSVRecord footer) {
            super(writeOperation, MimeTypes.TEXT);
            this.header = header;
            this.footer = footer;
        }

        /** Writes {@code value} followed by a newline character if {@code value} is not null. */
        private void writeIfNotNull(@Nullable CSVRecord value) throws IOException {
            if (value != null) {
                writeLine(value);
            }
        }

        /** Writes {@code value} followed by newline character. */
        private void writeLine(CSVRecord value) throws IOException {
            //TODO: write CSV Records change
            out.write(value.toString());
            out.write(NEWLINE);
        }

        @Override
        protected void prepareWrite(WritableByteChannel channel) throws Exception {
            out = new OutputStreamWriter(Channels.newOutputStream(channel), StandardCharsets.UTF_8);
        }

        @Override
        protected void writeHeader() throws Exception {
            writeIfNotNull(header);
        }

        @Override
        public void write(CSVRecord value) throws Exception {
            writeLine(value);
        }

        @Override
        protected void writeFooter() throws Exception {
            writeIfNotNull(footer);
        }

        @Override
        protected void finishWrite() throws Exception {
            out.flush();
        }
    }
}
