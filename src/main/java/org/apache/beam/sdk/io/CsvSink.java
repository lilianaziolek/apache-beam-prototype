/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io;

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

/**
 * Implementation detail of {@link CsvIO.Write}.
 *
 * <p>A {@link FileBasedSink} for text files. Produces text files with the newline separator {@code
 * '\n'} represented in {@code UTF-8} format as the record separator. Each record (including the
 * last) is terminated.
 */
class CsvSink<UserT, DestinationT> extends FileBasedSink<UserT, DestinationT, CSVRecord> {
  @Nullable private final CSVRecord header;
  @Nullable private final CSVRecord footer;

  CsvSink(
      ValueProvider<ResourceId> baseOutputFilename,
      DynamicDestinations<UserT, DestinationT, CSVRecord> dynamicDestinations,
      @Nullable CSVRecord header,
      @Nullable CSVRecord footer,
      WritableByteChannelFactory writableByteChannelFactory) {
    super(baseOutputFilename, dynamicDestinations, writableByteChannelFactory);
    this.header = header;
    this.footer = footer;
  }

  @Override
  public WriteOperation<DestinationT, CSVRecord> createWriteOperation() {
    return new CsvWriteOperation<>(this, header, footer);
  }

  /** A {@link WriteOperation WriteOperation} for text files. */
  private static class CsvWriteOperation<DestinationT>
      extends WriteOperation<DestinationT, CSVRecord> {
    @Nullable private final CSVRecord header;
    @Nullable private final CSVRecord footer;

    private CsvWriteOperation(CsvSink sink, @Nullable CSVRecord header, @Nullable CSVRecord footer) {
      super(sink);
      this.header = header;
      this.footer = footer;
    }

    @Override
    public Writer<DestinationT, CSVRecord> createWriter() throws Exception {
      return new CsvWriter<>(this, header, footer);
    }
  }

  /** A {@link Writer Writer} for text files. */
  private static class CsvWriter<DestinationT> extends Writer<DestinationT, CSVRecord> {
    private static final String NEWLINE = "\n";
    @Nullable private final CSVRecord header;
    @Nullable private final CSVRecord footer;

    // Initialized in prepareWrite
    @Nullable private OutputStreamWriter out;

    public CsvWriter(
        WriteOperation<DestinationT, CSVRecord> writeOperation,
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
      //TODO: use proper Csv writer
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
