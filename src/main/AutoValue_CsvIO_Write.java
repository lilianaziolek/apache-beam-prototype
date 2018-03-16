package com.db.awmd.lambda.csv2;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nullable;

//@Generated("com.google.auto.value.processor.AutoValueProcessor")
public class AutoValue_CsvIO_Write extends CsvIO.Write {

    private final ValueProvider<ResourceId> filenamePrefix;
    private final String filenameSuffix;
    private final CSVRecord header;
    private final CSVRecord footer;
    private final int numShards;
    private final String shardTemplate;
    private final FileBasedSink.FilenamePolicy filenamePolicy;
    private final boolean windowedWrites;
    private final FileBasedSink.WritableByteChannelFactory writableByteChannelFactory;

    private AutoValue_CsvIO_Write(
            @Nullable ValueProvider<ResourceId> filenamePrefix,
            @Nullable String filenameSuffix,
            @Nullable CSVRecord header,
            @Nullable CSVRecord footer,
            int numShards,
            @Nullable String shardTemplate,
            @Nullable FileBasedSink.FilenamePolicy filenamePolicy,
            boolean windowedWrites,
            FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.header = header;
        this.footer = footer;
        this.numShards = numShards;
        this.shardTemplate = shardTemplate;
        this.filenamePolicy = filenamePolicy;
        this.windowedWrites = windowedWrites;
        this.writableByteChannelFactory = writableByteChannelFactory;
    }

    @Nullable
    @Override
    ValueProvider<ResourceId> getFilenamePrefix() {
        return filenamePrefix;
    }

    @Nullable
    @Override
    String getFilenameSuffix() {
        return filenameSuffix;
    }

    @Nullable
    @Override
    CSVRecord getHeader() {
        return header;
    }

    @Nullable
    @Override
    CSVRecord getFooter() {
        return footer;
    }

    @Override
    int getNumShards() {
        return numShards;
    }

    @Nullable
    @Override
    String getShardTemplate() {
        return shardTemplate;
    }

    @Nullable
    @Override
    FileBasedSink.FilenamePolicy getFilenamePolicy() {
        return filenamePolicy;
    }

    @Override
    boolean getWindowedWrites() {
        return windowedWrites;
    }

    @Override
    FileBasedSink.WritableByteChannelFactory getWritableByteChannelFactory() {
        return writableByteChannelFactory;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof CsvIO.Write) {
            CsvIO.Write that = (CsvIO.Write) o;
            return ((this.filenamePrefix == null) ? (that.getFilenamePrefix() == null) : this.filenamePrefix.equals(that.getFilenamePrefix()))
                    && ((this.filenameSuffix == null) ? (that.getFilenameSuffix() == null) : this.filenameSuffix.equals(that.getFilenameSuffix()))
                    && ((this.header == null) ? (that.getHeader() == null) : this.header.equals(that.getHeader()))
                    && ((this.footer == null) ? (that.getFooter() == null) : this.footer.equals(that.getFooter()))
                    && (this.numShards == that.getNumShards())
                    && ((this.shardTemplate == null) ? (that.getShardTemplate() == null) : this.shardTemplate.equals(that.getShardTemplate()))
                    && ((this.filenamePolicy == null) ? (that.getFilenamePolicy() == null) : this.filenamePolicy.equals(that.getFilenamePolicy()))
                    && (this.windowedWrites == that.getWindowedWrites())
                    && (this.writableByteChannelFactory.equals(that.getWritableByteChannelFactory()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (filenamePrefix == null) ? 0 : this.filenamePrefix.hashCode();
        h *= 1000003;
        h ^= (filenameSuffix == null) ? 0 : this.filenameSuffix.hashCode();
        h *= 1000003;
        h ^= (header == null) ? 0 : this.header.hashCode();
        h *= 1000003;
        h ^= (footer == null) ? 0 : this.footer.hashCode();
        h *= 1000003;
        h ^= this.numShards;
        h *= 1000003;
        h ^= (shardTemplate == null) ? 0 : this.shardTemplate.hashCode();
        h *= 1000003;
        h ^= (filenamePolicy == null) ? 0 : this.filenamePolicy.hashCode();
        h *= 1000003;
        h ^= this.windowedWrites ? 1231 : 1237;
        h *= 1000003;
        h ^= this.writableByteChannelFactory.hashCode();
        return h;
    }

    @Override
    CsvIO.Write.Builder toBuilder() {
        return new AutoValue_CsvIO_Write.Builder(this);
    }

    static final class Builder extends CsvIO.Write.Builder {
        private ValueProvider<ResourceId> filenamePrefix;
        private String filenameSuffix;
        private CSVRecord header;
        private CSVRecord footer;
        private Integer numShards;
        private String shardTemplate;
        private FileBasedSink.FilenamePolicy filenamePolicy;
        private Boolean windowedWrites;
        private FileBasedSink.WritableByteChannelFactory writableByteChannelFactory;
        Builder() {
        }
        private Builder(CsvIO.Write source) {
            this.filenamePrefix = source.getFilenamePrefix();
            this.filenameSuffix = source.getFilenameSuffix();
            this.header = source.getHeader();
            this.footer = source.getFooter();
            this.numShards = source.getNumShards();
            this.shardTemplate = source.getShardTemplate();
            this.filenamePolicy = source.getFilenamePolicy();
            this.windowedWrites = source.getWindowedWrites();
            this.writableByteChannelFactory = source.getWritableByteChannelFactory();
        }
        @Override
        CsvIO.Write.Builder setFilenamePrefix(@Nullable ValueProvider<ResourceId> filenamePrefix) {
            this.filenamePrefix = filenamePrefix;
            return this;
        }
        @Override
        CsvIO.Write.Builder setFilenameSuffix(@Nullable String filenameSuffix) {
            this.filenameSuffix = filenameSuffix;
            return this;
        }
        @Override
        CsvIO.Write.Builder setHeader(@Nullable CSVRecord header) {
            this.header = header;
            return this;
        }
        @Override
        CsvIO.Write.Builder setFooter(@Nullable CSVRecord footer) {
            this.footer = footer;
            return this;
        }
        @Override
        CsvIO.Write.Builder setNumShards(int numShards) {
            this.numShards = numShards;
            return this;
        }
        @Override
        CsvIO.Write.Builder setShardTemplate(@Nullable String shardTemplate) {
            this.shardTemplate = shardTemplate;
            return this;
        }
        @Override
        CsvIO.Write.Builder setFilenamePolicy(@Nullable FileBasedSink.FilenamePolicy filenamePolicy) {
            this.filenamePolicy = filenamePolicy;
            return this;
        }
        @Override
        CsvIO.Write.Builder setWindowedWrites(boolean windowedWrites) {
            this.windowedWrites = windowedWrites;
            return this;
        }
        @Override
        CsvIO.Write.Builder setWritableByteChannelFactory(FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
            if (writableByteChannelFactory == null) {
                throw new NullPointerException("Null writableByteChannelFactory");
            }
            this.writableByteChannelFactory = writableByteChannelFactory;
            return this;
        }
        @Override
        CsvIO.Write build() {
            String missing = "";
            if (this.numShards == null) {
                missing += " numShards";
            }
            if (this.windowedWrites == null) {
                missing += " windowedWrites";
            }
            if (this.writableByteChannelFactory == null) {
                missing += " writableByteChannelFactory";
            }
            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required properties:" + missing);
            }
            return new AutoValue_CsvIO_Write(
                    this.filenamePrefix,
                    this.filenameSuffix,
                    this.header,
                    this.footer,
                    this.numShards,
                    this.shardTemplate,
                    this.filenamePolicy,
                    this.windowedWrites,
                    this.writableByteChannelFactory);
        }
    }

}
