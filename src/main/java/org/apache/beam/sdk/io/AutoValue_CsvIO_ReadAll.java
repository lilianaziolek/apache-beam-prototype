
package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import java.util.Arrays;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_CsvIO_ReadAll extends CsvIO.ReadAll {

  private final FileIO.MatchConfiguration matchConfiguration;
  private final Compression compression;

  private AutoValue_CsvIO_ReadAll(FileIO.MatchConfiguration matchConfiguration, Compression compression) {
    this.matchConfiguration = matchConfiguration;
    this.compression = compression;
  }

  @Override
  FileIO.MatchConfiguration getMatchConfiguration() {
    return matchConfiguration;
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
    if (o instanceof CsvIO.ReadAll) {
      CsvIO.ReadAll that = (CsvIO.ReadAll) o;
      return (this.matchConfiguration.equals(that.getMatchConfiguration()))
           && (this.compression.equals(that.getCompression()))
              ;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= this.matchConfiguration.hashCode();
    h *= 1000003;
    h ^= this.compression.hashCode();
    return h;
  }

  @Override
  CsvIO.ReadAll.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends CsvIO.ReadAll.Builder {
    private FileIO.MatchConfiguration matchConfiguration;
    private Compression compression;
    Builder() {
    }
    private Builder(CsvIO.ReadAll source) {
      this.matchConfiguration = source.getMatchConfiguration();
      this.compression = source.getCompression();
    }
    @Override
    CsvIO.ReadAll.Builder setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    CsvIO.ReadAll.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }

    @Override
    CsvIO.ReadAll build() {
      String missing = "";
      if (this.matchConfiguration == null) {
        missing += " matchConfiguration";
      }
      if (this.compression == null) {
        missing += " compression";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_CsvIO_ReadAll(
          this.matchConfiguration,
          this.compression);
    }
  }

}
