
package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import java.util.Arrays;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_CsvIO_ReadFiles extends CsvIO.ReadFiles {

  private final long desiredBundleSizeBytes;

  private AutoValue_CsvIO_ReadFiles(long desiredBundleSizeBytes) {
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
  }

  @Override
  long getDesiredBundleSizeBytes() {
    return desiredBundleSizeBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CsvIO.ReadFiles) {
      CsvIO.ReadFiles that = (CsvIO.ReadFiles) o;
      return (this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes())
              ;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= (int) ((this.desiredBundleSizeBytes >>> 32) ^ this.desiredBundleSizeBytes);
    return h;
  }

  @Override
  CsvIO.ReadFiles.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends CsvIO.ReadFiles.Builder {
    private Long desiredBundleSizeBytes;
    Builder() {
    }
    private Builder(CsvIO.ReadFiles source) {
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
    }
    @Override
    CsvIO.ReadFiles.Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      return this;
    }

    @Override
    CsvIO.ReadFiles build() {
      String missing = "";
      if (this.desiredBundleSizeBytes == null) {
        missing += " desiredBundleSizeBytes";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_CsvIO_ReadFiles(
          this.desiredBundleSizeBytes);
    }
  }

}
