package com.db.awmd.lambda.csv2;

import org.apache.beam.sdk.options.ValueProvider;

import javax.annotation.Nullable;

//@Generated("com.google.auto.value.processor.AutoValueProcessor")
public class AutoValue_CsvIO_Read extends CsvIO.Read {

    private final ValueProvider<String> filepattern;
    private final CsvIO.CompressionType compressionType;

    private AutoValue_CsvIO_Read(
            @Nullable ValueProvider<String> filepattern,
            CsvIO.CompressionType compressionType) {
        this.filepattern = filepattern;
        this.compressionType = compressionType;
    }

    @Nullable
    @Override
    ValueProvider<String> getFilepattern() {
        return filepattern;
    }

    @Override
    CsvIO.CompressionType getCompressionType() {
        return compressionType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof CsvIO.Read) {
            CsvIO.Read that = (CsvIO.Read) o;
            return ((this.filepattern == null) ? (that.getFilepattern() == null) : this.filepattern.equals(that.getFilepattern()))
                    && (this.compressionType.equals(that.getCompressionType()));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= (filepattern == null) ? 0 : this.filepattern.hashCode();
        h *= 1000003;
        h ^= this.compressionType.hashCode();
        return h;
    }

    @Override
    CsvIO.Read.Builder toBuilder() {
        return new AutoValue_CsvIO_Read.Builder(this);
    }

    static final class Builder extends CsvIO.Read.Builder {
        private ValueProvider<String> filepattern;
        private CsvIO.CompressionType compressionType;
        Builder() {
        }
        private Builder(CsvIO.Read source) {
            this.filepattern = source.getFilepattern();
            this.compressionType = source.getCompressionType();
        }
        @Override
        CsvIO.Read.Builder setFilepattern(@Nullable ValueProvider<String> filepattern) {
            this.filepattern = filepattern;
            return this;
        }
        @Override
        CsvIO.Read.Builder setCompressionType(CsvIO.CompressionType compressionType) {
            if (compressionType == null) {
                throw new NullPointerException("Null compressionType");
            }
            this.compressionType = compressionType;
            return this;
        }
        @Override
        CsvIO.Read build() {
            String missing = "";
            if (this.compressionType == null) {
                missing += " compressionType";
            }
            if (!missing.isEmpty()) {
                throw new IllegalStateException("Missing required properties:" + missing);
            }
            return new AutoValue_CsvIO_Read(
                    this.filepattern,
                    this.compressionType);
        }
    }
}
