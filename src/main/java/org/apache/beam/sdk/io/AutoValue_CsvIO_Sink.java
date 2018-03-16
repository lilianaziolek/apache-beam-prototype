
package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import javax.annotation.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_CsvIO_Sink extends CsvIO.Sink {

  private final String header;
  private final String footer;

  private AutoValue_CsvIO_Sink(
      @Nullable String header,
      @Nullable String footer) {
    this.header = header;
    this.footer = footer;
  }

  @Nullable
  @Override
  String getHeader() {
    return header;
  }

  @Nullable
  @Override
  String getFooter() {
    return footer;
  }

  @Override
  public String toString() {
    return "Sink{"
        + "header=" + header + ", "
        + "footer=" + footer
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CsvIO.Sink) {
      CsvIO.Sink that = (CsvIO.Sink) o;
      return ((this.header == null) ? (that.getHeader() == null) : this.header.equals(that.getHeader()))
           && ((this.footer == null) ? (that.getFooter() == null) : this.footer.equals(that.getFooter()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= (header == null) ? 0 : this.header.hashCode();
    h *= 1000003;
    h ^= (footer == null) ? 0 : this.footer.hashCode();
    return h;
  }

  @Override
  CsvIO.Sink.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends CsvIO.Sink.Builder {
    private String header;
    private String footer;
    Builder() {
    }
    private Builder(CsvIO.Sink source) {
      this.header = source.getHeader();
      this.footer = source.getFooter();
    }
    @Override
    CsvIO.Sink.Builder setHeader(@Nullable String header) {
      this.header = header;
      return this;
    }
    @Override
    CsvIO.Sink.Builder setFooter(@Nullable String footer) {
      this.footer = footer;
      return this;
    }
    @Override
    CsvIO.Sink build() {
      return new AutoValue_CsvIO_Sink(
          this.header,
          this.footer);
    }
  }

}
