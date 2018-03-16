
package org.apache.beam.sdk.io;

import org.apache.beam.sdk.options.ValueProvider;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import java.util.Arrays;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_CsvIO_Read extends CsvIO.Read {

  private final ValueProvider<String> filepattern;
  private final FileIO.MatchConfiguration matchConfiguration;
  private final boolean hintMatchesManyFiles;
  private final Compression compression;

  private AutoValue_CsvIO_Read(@Nullable ValueProvider<String> filepattern, FileIO.MatchConfiguration matchConfiguration, boolean hintMatchesManyFiles,
          Compression compression) {
    this.filepattern = filepattern;
    this.matchConfiguration = matchConfiguration;
    this.hintMatchesManyFiles = hintMatchesManyFiles;
    this.compression = compression;
  }

  @Nullable
  @Override
  ValueProvider<String> getFilepattern() {
    return filepattern;
  }

  @Override
  FileIO.MatchConfiguration getMatchConfiguration() {
    return matchConfiguration;
  }

  @Override
  boolean getHintMatchesManyFiles() {
    return hintMatchesManyFiles;
  }

  @Override
  Compression getCompression() {
    return compression;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CsvIO.Read) {
      CsvIO.Read that = (CsvIO.Read) o;
      return ((this.filepattern == null) ? (that.getFilepattern() == null) : this.filepattern.equals(that.getFilepattern()))
           && (this.matchConfiguration.equals(that.getMatchConfiguration()))
           && (this.hintMatchesManyFiles == that.getHintMatchesManyFiles())
           && (this.compression.equals(that.getCompression()))
              ;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= (filepattern == null) ? 0 : this.filepattern.hashCode();
    h *= 1000003;
    h ^= this.matchConfiguration.hashCode();
    h *= 1000003;
    h ^= this.hintMatchesManyFiles ? 1231 : 1237;
    h *= 1000003;
    h ^= this.compression.hashCode();
    return h;
  }

  @Override
  CsvIO.Read.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends CsvIO.Read.Builder {
    private ValueProvider<String> filepattern;
    private FileIO.MatchConfiguration matchConfiguration;
    private Boolean hintMatchesManyFiles;
    private Compression compression;
    Builder() {
    }
    private Builder(CsvIO.Read source) {
      this.filepattern = source.getFilepattern();
      this.matchConfiguration = source.getMatchConfiguration();
      this.hintMatchesManyFiles = source.getHintMatchesManyFiles();
      this.compression = source.getCompression();
    }
    @Override
    CsvIO.Read.Builder setFilepattern(@Nullable ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    CsvIO.Read.Builder setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    CsvIO.Read.Builder setHintMatchesManyFiles(boolean hintMatchesManyFiles) {
      this.hintMatchesManyFiles = hintMatchesManyFiles;
      return this;
    }
    @Override
    CsvIO.Read.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }

    @Override
    CsvIO.Read build() {
      String missing = "";
      if (this.matchConfiguration == null) {
        missing += " matchConfiguration";
      }
      if (this.hintMatchesManyFiles == null) {
        missing += " hintMatchesManyFiles";
      }
      if (this.compression == null) {
        missing += " compression";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_CsvIO_Read(
          this.filepattern,
          this.matchConfiguration,
          this.hintMatchesManyFiles,
          this.compression);
    }
  }

}
