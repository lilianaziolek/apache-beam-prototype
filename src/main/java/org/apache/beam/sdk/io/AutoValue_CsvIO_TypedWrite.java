
package org.apache.beam.sdk.io;

import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.csv.CSVRecord;

import javax.annotation.Generated;
import javax.annotation.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
 final class AutoValue_CsvIO_TypedWrite<UserT, DestinationT> extends CsvIO.TypedWrite<UserT, DestinationT> {

  private final ValueProvider<ResourceId> filenamePrefix;
  private final String filenameSuffix;
  private final ValueProvider<ResourceId> tempDirectory;
  private final CSVRecord header;
  private final CSVRecord footer;
  private final int numShards;
  private final String shardTemplate;
  private final FileBasedSink.FilenamePolicy filenamePolicy;
  private final FileBasedSink.DynamicDestinations<UserT, DestinationT, CSVRecord> dynamicDestinations;
  private final SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction;
  private final DefaultFilenamePolicy.Params emptyDestination;
  private final SerializableFunction<UserT, CSVRecord> formatFunction;
  private final boolean windowedWrites;
  private final FileBasedSink.WritableByteChannelFactory writableByteChannelFactory;

  private AutoValue_CsvIO_TypedWrite(
      @Nullable ValueProvider<ResourceId> filenamePrefix,
      @Nullable String filenameSuffix,
      @Nullable ValueProvider<ResourceId> tempDirectory,
      @Nullable CSVRecord header,
      @Nullable CSVRecord footer,
      int numShards,
      @Nullable String shardTemplate,
      @Nullable FileBasedSink.FilenamePolicy filenamePolicy,
      @Nullable FileBasedSink.DynamicDestinations<UserT, DestinationT, CSVRecord> dynamicDestinations,
      @Nullable SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction,
      @Nullable DefaultFilenamePolicy.Params emptyDestination,
      @Nullable SerializableFunction<UserT, CSVRecord> formatFunction,
      boolean windowedWrites,
      FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
    this.filenamePrefix = filenamePrefix;
    this.filenameSuffix = filenameSuffix;
    this.tempDirectory = tempDirectory;
    this.header = header;
    this.footer = footer;
    this.numShards = numShards;
    this.shardTemplate = shardTemplate;
    this.filenamePolicy = filenamePolicy;
    this.dynamicDestinations = dynamicDestinations;
    this.destinationFunction = destinationFunction;
    this.emptyDestination = emptyDestination;
    this.formatFunction = formatFunction;
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
  ValueProvider<ResourceId> getTempDirectory() {
    return tempDirectory;
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

  @Nullable
  @Override
  FileBasedSink.DynamicDestinations<UserT, DestinationT, CSVRecord> getDynamicDestinations() {
    return dynamicDestinations;
  }

  @Nullable
  @Override
  SerializableFunction<UserT, DefaultFilenamePolicy.Params> getDestinationFunction() {
    return destinationFunction;
  }

  @Nullable
  @Override
  DefaultFilenamePolicy.Params getEmptyDestination() {
    return emptyDestination;
  }

  @Nullable
  @Override
  SerializableFunction<UserT, CSVRecord> getFormatFunction() {
    return formatFunction;
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
    if (o instanceof CsvIO.TypedWrite) {
      CsvIO.TypedWrite<?, ?> that = (CsvIO.TypedWrite<?, ?>) o;
      return ((this.filenamePrefix == null) ? (that.getFilenamePrefix() == null) : this.filenamePrefix.equals(that.getFilenamePrefix()))
           && ((this.filenameSuffix == null) ? (that.getFilenameSuffix() == null) : this.filenameSuffix.equals(that.getFilenameSuffix()))
           && ((this.tempDirectory == null) ? (that.getTempDirectory() == null) : this.tempDirectory.equals(that.getTempDirectory()))
           && ((this.header == null) ? (that.getHeader() == null) : this.header.equals(that.getHeader()))
           && ((this.footer == null) ? (that.getFooter() == null) : this.footer.equals(that.getFooter()))
           && (this.numShards == that.getNumShards())
           && ((this.shardTemplate == null) ? (that.getShardTemplate() == null) : this.shardTemplate.equals(that.getShardTemplate()))
           && ((this.filenamePolicy == null) ? (that.getFilenamePolicy() == null) : this.filenamePolicy.equals(that.getFilenamePolicy()))
           && ((this.dynamicDestinations == null) ? (that.getDynamicDestinations() == null) : this.dynamicDestinations.equals(that.getDynamicDestinations()))
           && ((this.destinationFunction == null) ? (that.getDestinationFunction() == null) : this.destinationFunction.equals(that.getDestinationFunction()))
           && ((this.emptyDestination == null) ? (that.getEmptyDestination() == null) : this.emptyDestination.equals(that.getEmptyDestination()))
           && ((this.formatFunction == null) ? (that.getFormatFunction() == null) : this.formatFunction.equals(that.getFormatFunction()))
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
    h ^= (tempDirectory == null) ? 0 : this.tempDirectory.hashCode();
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
    h ^= (dynamicDestinations == null) ? 0 : this.dynamicDestinations.hashCode();
    h *= 1000003;
    h ^= (destinationFunction == null) ? 0 : this.destinationFunction.hashCode();
    h *= 1000003;
    h ^= (emptyDestination == null) ? 0 : this.emptyDestination.hashCode();
    h *= 1000003;
    h ^= (formatFunction == null) ? 0 : this.formatFunction.hashCode();
    h *= 1000003;
    h ^= this.windowedWrites ? 1231 : 1237;
    h *= 1000003;
    h ^= this.writableByteChannelFactory.hashCode();
    return h;
  }

  @Override
  CsvIO.TypedWrite.Builder<UserT, DestinationT> toBuilder() {
    return new Builder<UserT, DestinationT>(this);
  }

  static final class Builder<UserT, DestinationT> extends CsvIO.TypedWrite.Builder<UserT, DestinationT> {
    private ValueProvider<ResourceId> filenamePrefix;
    private String filenameSuffix;
    private ValueProvider<ResourceId> tempDirectory;
    private CSVRecord header;
    private CSVRecord footer;
    private Integer numShards;
    private String shardTemplate;
    private FileBasedSink.FilenamePolicy filenamePolicy;
    private FileBasedSink.DynamicDestinations<UserT, DestinationT, CSVRecord> dynamicDestinations;
    private SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction;
    private DefaultFilenamePolicy.Params emptyDestination;
    private SerializableFunction<UserT, CSVRecord> formatFunction;
    private Boolean windowedWrites;
    private FileBasedSink.WritableByteChannelFactory writableByteChannelFactory;
    Builder() {
    }
    private Builder(CsvIO.TypedWrite<UserT, DestinationT> source) {
      this.filenamePrefix = source.getFilenamePrefix();
      this.filenameSuffix = source.getFilenameSuffix();
      this.tempDirectory = source.getTempDirectory();
      this.header = source.getHeader();
      this.footer = source.getFooter();
      this.numShards = source.getNumShards();
      this.shardTemplate = source.getShardTemplate();
      this.filenamePolicy = source.getFilenamePolicy();
      this.dynamicDestinations = source.getDynamicDestinations();
      this.destinationFunction = source.getDestinationFunction();
      this.emptyDestination = source.getEmptyDestination();
      this.formatFunction = source.getFormatFunction();
      this.windowedWrites = source.getWindowedWrites();
      this.writableByteChannelFactory = source.getWritableByteChannelFactory();
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setFilenamePrefix(@Nullable ValueProvider<ResourceId> filenamePrefix) {
      this.filenamePrefix = filenamePrefix;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setFilenameSuffix(@Nullable String filenameSuffix) {
      this.filenameSuffix = filenameSuffix;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setTempDirectory(@Nullable ValueProvider<ResourceId> tempDirectory) {
      this.tempDirectory = tempDirectory;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setHeader(@Nullable CSVRecord header) {
      this.header = header;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setFooter(@Nullable CSVRecord footer) {
      this.footer = footer;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setNumShards(int numShards) {
      this.numShards = numShards;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setShardTemplate(@Nullable String shardTemplate) {
      this.shardTemplate = shardTemplate;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setFilenamePolicy(@Nullable FileBasedSink.FilenamePolicy filenamePolicy) {
      this.filenamePolicy = filenamePolicy;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setDynamicDestinations(@Nullable FileBasedSink.DynamicDestinations<UserT, DestinationT, CSVRecord> dynamicDestinations) {
      this.dynamicDestinations = dynamicDestinations;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setDestinationFunction(@Nullable SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction) {
      this.destinationFunction = destinationFunction;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setEmptyDestination(@Nullable DefaultFilenamePolicy.Params emptyDestination) {
      this.emptyDestination = emptyDestination;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setFormatFunction(@Nullable SerializableFunction<UserT, CSVRecord> formatFunction) {
      this.formatFunction = formatFunction;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
      return this;
    }
    @Override
    CsvIO.TypedWrite.Builder<UserT, DestinationT> setWritableByteChannelFactory(FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
      if (writableByteChannelFactory == null) {
        throw new NullPointerException("Null writableByteChannelFactory");
      }
      this.writableByteChannelFactory = writableByteChannelFactory;
      return this;
    }
    @Override
    CsvIO.TypedWrite<UserT, DestinationT> build() {
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
      return new AutoValue_CsvIO_TypedWrite<UserT, DestinationT>(
          this.filenamePrefix,
          this.filenameSuffix,
          this.tempDirectory,
          this.header,
          this.footer,
          this.numShards,
          this.shardTemplate,
          this.filenamePolicy,
          this.dynamicDestinations,
          this.destinationFunction,
          this.emptyDestination,
          this.formatFunction,
          this.windowedWrites,
          this.writableByteChannelFactory);
    }
  }

}
