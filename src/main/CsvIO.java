package com.db.awmd.lambda.csv2;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.csv.CSVRecord;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.*;

/**
 * {@link PTransform}s for reading and writing text files.
 * <p>
 * <p>To read a {@link PCollection} from one or more text files, use {@code Csv2IO.read()} to
 * instantiate a transform and use {@link CsvIO.Read#from(String)} to specify the path of the
 * file(s) to be read.
 * <p>
 * <p>{@link CsvIO.Read} returns a {@link PCollection} of {@link CSVRecord csv records}, using Apache CSV Commons library for reading records.
 * <p>
 * <p>Example:
 * <p>
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<CSVRecord> lines = p.apply(Csv2IO.read().from("/local/path/to/file.txt"));
 * }</pre>
 * <p>
 * <p>To write a {@link PCollection} to one or more text files, use {@code Csv2IO.write()}, using
 * {@link CsvIO.Write#to(String)} to specify the output prefix of the files to write.
 * <p>
 * <p>By default, all input is put into the global window before writing. If per-window writes are
 * desired - for example, when using a streaming runner -
 * {@link CsvIO.Write#withWindowedWrites()} will cause windowing and triggering to be
 * preserved. When producing windowed writes, the number of output shards must be set explicitly
 * using {@link CsvIO.Write#withNumShards(int)}; some runners may set this for you to a
 * runner-chosen value, so you may need not set it yourself. A {@link FileBasedSink.FilenamePolicy} can also be
 * set in case you need better control over naming files created by unique windows.
 * {@link DefaultFilenamePolicy} policy for producing unique filenames might not be appropriate
 * for your use case.
 * <p>
 * <p>Any existing files with the same names as generated output files will be overwritten.
 * <p>
 * <p>For example:
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<CSVRecord> lines = ...;
 * lines.apply(Csv2IO.write().to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<CSVRecord> lines = ...;
 * lines.apply(Csv2IO.write().to("/path/to/file.txt"));
 *      .withSuffix(".txt")
 *      .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
 * }</pre>
 */

public class CsvIO {
    /**
     * A {@link PTransform} that reads from one or more text files and returns a bounded
     * {@link PCollection} containing one element for each line of the input files.
     */
    public static CsvIO.Read read() {
        return new AutoValue_CsvIO_Read.Builder().setCompressionType(CsvIO.CompressionType.AUTO).build();
    }

    /**
     * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
     * matching a sharding pattern), with each element of the input collection encoded into its own
     * line.
     */
    public static CsvIO.Write write() {
        return new AutoValue_CsvIO_Write.Builder().setFilenamePrefix(null)
                .setShardTemplate(null)
                .setFilenameSuffix(null)
                .setFilenamePolicy(null)
                .setWritableByteChannelFactory(FileBasedSink.CompressionType.UNCOMPRESSED)
                .setWindowedWrites(false)
                .setNumShards(0)
                .build();
    }

    /**
     * Implementation of {@link #read}.
     */
    @AutoValue
    public abstract static class Read extends PTransform<PBegin, PCollection<CSVRecord>> {
        @Nullable
        abstract ValueProvider<String> getFilepattern();

        abstract CsvIO.CompressionType getCompressionType();

        abstract CsvIO.Read.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract CsvIO.Read.Builder setFilepattern(ValueProvider<String> filepattern);

            abstract CsvIO.Read.Builder setCompressionType(CsvIO.CompressionType compressionType);

            abstract CsvIO.Read build();
        }

        /**
         * Reads text files that reads from the file(s) with the given filename or filename pattern.
         * <p>
         * <p>This can be a local path (if running locally), or a Google Cloud Storage filename or
         * filename pattern of the form {@code "gs://<bucket>/<filepath>"} (if running locally or using
         * remote execution service).
         * <p>
         * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
         * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
         */
        public CsvIO.Read from(String filepattern) {
            checkNotNull(filepattern, "Filepattern cannot be empty.");
            return from(ValueProvider.StaticValueProvider.of(filepattern));
        }

        /**
         * Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}.
         */
        public CsvIO.Read from(ValueProvider<String> filepattern) {
            checkNotNull(filepattern, "Filepattern cannot be empty.");
            return toBuilder().setFilepattern(filepattern).build();
        }

        /**
         * Returns a new transform for reading from text files that's like this one but
         * reads from input sources using the specified compression type.
         * <p>
         * <p>If no compression type is specified, the default is {@link CsvIO.CompressionType#AUTO}.
         */
        public CsvIO.Read withCompressionType(CsvIO.CompressionType compressionType) {
            return toBuilder().setCompressionType(compressionType).build();
        }

        @Override
        public PCollection<CSVRecord> expand(PBegin input) {
            if (getFilepattern() == null) {
                throw new IllegalStateException("need to set the filepattern of a Csv2IO.Read transform");
            }

            final Bounded<CSVRecord> read = org.apache.beam.sdk.io.Read.from(getSource());
            PCollection<CSVRecord> pcol = input.getPipeline().apply("Read", read);
            // Honor the default output coder that would have been used by this PTransform.
            pcol.setCoder(getDefaultOutputCoder());
            return pcol;
        }

        // Helper to create a source specific to the requested compression type.
        protected FileBasedSource<CSVRecord> getSource() {
            switch (getCompressionType()) {
            case UNCOMPRESSED:
                return new CsvSource(getFilepattern());
            case AUTO:
                return CompressedSource.from(new CsvSource(getFilepattern()));
            case BZIP2:
                return CompressedSource.from(new CsvSource(getFilepattern())).withDecompression(CompressedSource.CompressionMode.BZIP2);
            case GZIP:
                return CompressedSource.from(new CsvSource(getFilepattern())).withDecompression(CompressedSource.CompressionMode.GZIP);
            case ZIP:
                return CompressedSource.from(new CsvSource(getFilepattern())).withDecompression(CompressedSource.CompressionMode.ZIP);
            case DEFLATE:
                return CompressedSource.from(new CsvSource(getFilepattern())).withDecompression(CompressedSource.CompressionMode.DEFLATE);
            default:
                throw new IllegalArgumentException("Unknown compression type: " + getFilepattern());
            }
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);

            String filepatternDisplay = getFilepattern().isAccessible() ? getFilepattern().get() : getFilepattern().toString();
            builder.add(DisplayData.item("compressionType", getCompressionType().toString()).withLabel("Compression Type"))
                    .addIfNotNull(DisplayData.item("filePattern", filepatternDisplay).withLabel("File Pattern"));
        }

        @Override
        protected Coder<CSVRecord> getDefaultOutputCoder() {
            return CsvCoder.of();
        }
    }

    /////////////////////////////////////////////////////////////////////////////

    /**
     * Implementation of {@link #write}.
     */
    @AutoValue
    public abstract static class Write extends PTransform<PCollection<CSVRecord>, PDone> {
        /**
         * The prefix of each file written, combined with suffix and shardTemplate.
         */
        @Nullable
        abstract ValueProvider<ResourceId> getFilenamePrefix();

        /**
         * The suffix of each file written, combined with prefix and shardTemplate.
         */
        @Nullable
        abstract String getFilenameSuffix();

        /**
         * An optional header to add to each file.
         */
        @Nullable
        abstract CSVRecord getHeader();

        /**
         * An optional footer to add to each file.
         */
        @Nullable
        abstract CSVRecord getFooter();

        /**
         * Requested number of shards. 0 for automatic.
         */
        abstract int getNumShards();

        /**
         * The shard template of each file written, combined with prefix and suffix.
         */
        @Nullable
        abstract String getShardTemplate();

        /**
         * A policy for naming output files.
         */
        @Nullable
        abstract FileBasedSink.FilenamePolicy getFilenamePolicy();

        /**
         * Whether to write windowed output files.
         */
        abstract boolean getWindowedWrites();

        /**
         * The {@link FileBasedSink.WritableByteChannelFactory} to be used by the {@link FileBasedSink}. Default is
         * {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
         */
        abstract FileBasedSink.WritableByteChannelFactory getWritableByteChannelFactory();

        abstract CsvIO.Write.Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract CsvIO.Write.Builder setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix);

            abstract CsvIO.Write.Builder setShardTemplate(@Nullable String shardTemplate);

            abstract CsvIO.Write.Builder setFilenameSuffix(@Nullable String filenameSuffix);

            abstract CsvIO.Write.Builder setHeader(@Nullable CSVRecord header);

            abstract CsvIO.Write.Builder setFooter(@Nullable CSVRecord footer);

            abstract CsvIO.Write.Builder setFilenamePolicy(@Nullable FileBasedSink.FilenamePolicy filenamePolicy);

            abstract CsvIO.Write.Builder setNumShards(int numShards);

            abstract CsvIO.Write.Builder setWindowedWrites(boolean windowedWrites);

            abstract CsvIO.Write.Builder setWritableByteChannelFactory(FileBasedSink.WritableByteChannelFactory writableByteChannelFactory);

            abstract CsvIO.Write build();
        }

        /**
         * Writes to text files with the given prefix. The given {@code prefix} can reference any
         * {@link FileSystem} on the classpath.
         * <p>
         * <p>The name of the output files will be determined by the {@link FileBasedSink.FilenamePolicy} used.
         * <p>
         * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
         * to define the base output directory and file prefix, a shard identifier (see
         * {@link #withNumShards(int)}), and a common suffix (if supplied using
         * {@link #withSuffix(String)}).
         * <p>
         * <p>This default policy can be overridden using {@link #withFilenamePolicy(FileBasedSink.FilenamePolicy)},
         * in which case {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should
         * not be set.
         */
        public CsvIO.Write to(String filenamePrefix) {
            return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
        }

        /**
         * Writes to text files with prefix from the given resource.
         * <p>
         * <p>The name of the output files will be determined by the {@link FileBasedSink.FilenamePolicy} used.
         * <p>
         * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
         * to define the base output directory and file prefix, a shard identifier (see
         * {@link #withNumShards(int)}), and a common suffix (if supplied using
         * {@link #withSuffix(String)}).
         * <p>
         * <p>This default policy can be overridden using {@link #withFilenamePolicy(FileBasedSink.FilenamePolicy)},
         * in which case {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should
         * not be set.
         */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public CsvIO.Write to(ResourceId filenamePrefix) {
            return toResource(ValueProvider.StaticValueProvider.of(filenamePrefix));
        }

        /**
         * Like {@link #to(String)}.
         */
        public CsvIO.Write to(ValueProvider<String> outputPrefix) {
            return toResource(ValueProvider.NestedValueProvider.of(outputPrefix, new SerializableFunction<String, ResourceId>() {
                @Override
                public ResourceId apply(String input) {
                    return FileBasedSink.convertToFileResourceIfPossible(input);
                }
            }));
        }

        /**
         * Like {@link #to(ResourceId)}.
         */
        @Experimental(Experimental.Kind.FILESYSTEM)
        public CsvIO.Write toResource(ValueProvider<ResourceId> filenamePrefix) {
            return toBuilder().setFilenamePrefix(filenamePrefix).build();
        }

        /**
         * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
         * used when {@link #withFilenamePolicy(FileBasedSink.FilenamePolicy)} has not been configured.
         * <p>
         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
         * used.
         */
        public CsvIO.Write withShardNameTemplate(String shardTemplate) {
            return toBuilder().setShardTemplate(shardTemplate).build();
        }

        /**
         * Configures the filename suffix for written files. This option may only be used when
         * {@link #withFilenamePolicy(FileBasedSink.FilenamePolicy)} has not been configured.
         * <p>
         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
         * used.
         */
        public CsvIO.Write withSuffix(String filenameSuffix) {
            return toBuilder().setFilenameSuffix(filenameSuffix).build();
        }

        /**
         * Configures the {@link FileBasedSink.FilenamePolicy} that will be used to name written files.
         */
        public CsvIO.Write withFilenamePolicy(FileBasedSink.FilenamePolicy filenamePolicy) {
            return toBuilder().setFilenamePolicy(filenamePolicy).build();
        }

        /**
         * Configures the number of output shards produced overall (when using unwindowed writes) or
         * per-window (when using windowed writes).
         * <p>
         * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
         * performance of a pipeline. Setting this value is not recommended unless you require a
         * specific number of output files.
         *
         * @param numShards the number of shards to use, or 0 to let the system decide.
         */
        public CsvIO.Write withNumShards(int numShards) {
            checkArgument(numShards >= 0);
            return toBuilder().setNumShards(numShards).build();
        }

        /**
         * Forces a single file as output and empty shard name template. This option is only compatible
         * with unwindowed writes.
         * <p>
         * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
         * performance of a pipeline. Setting this value is not recommended unless you require a
         * specific number of output files.
         * <p>
         * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
         */
        public CsvIO.Write withoutSharding() {
            return withNumShards(1).withShardNameTemplate("");
        }

        /**
         * Adds a header string to each file. A newline after the header is added automatically.
         * <p>
         * <p>A {@code null} value will clear any previously configured header.
         */
        public CsvIO.Write withHeader(@Nullable CSVRecord header) {
            return toBuilder().setHeader(header).build();
        }

        /**
         * Adds a footer string to each file. A newline after the footer is added automatically.
         * <p>
         * <p>A {@code null} value will clear any previously configured footer.
         */
        public CsvIO.Write withFooter(@Nullable CSVRecord footer) {
            return toBuilder().setFooter(footer).build();
        }

        /**
         * Returns a transform for writing to text files like this one but that has the given
         * {@link FileBasedSink.WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output.
         * The default is value is {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
         * <p>
         * <p>A {@code null} value will reset the value to the default value mentioned above.
         */
        public CsvIO.Write withWritableByteChannelFactory(FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
            return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
        }

        public CsvIO.Write withWindowedWrites() {
            return toBuilder().setWindowedWrites(true).build();
        }

        @Override
        public PDone expand(PCollection<CSVRecord> input) {
            checkState(getFilenamePrefix() != null, "Need to set the filename prefix of a Csv2IO.Write transform.");
            checkState((getFilenamePolicy() == null) || (getShardTemplate() == null && getFilenameSuffix() == null),
                    "Cannot set a filename policy and also a filename template or suffix.");

            FileBasedSink.FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
            if (usedFilenamePolicy == null) {
                usedFilenamePolicy = DefaultFilenamePolicy.constructUsingStandardParameters(getFilenamePrefix(), getShardTemplate(),
                        getFilenameSuffix(), getWindowedWrites());
            }
            WriteFiles<CSVRecord> write = WriteFiles.to(
                    new CsvSink(getFilenamePrefix(), usedFilenamePolicy, getHeader(), getFooter(), getWritableByteChannelFactory()));
            if (getNumShards() > 0) {
                write = write.withNumShards(getNumShards());
            }
            if (getWindowedWrites()) {
                write = write.withWindowedWrites();
            }
            return input.apply("WriteFiles", write);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);

            String prefixString = "";
            if (getFilenamePrefix() != null) {
                prefixString = getFilenamePrefix().isAccessible() ? getFilenamePrefix().get().toString() : getFilenamePrefix().toString();
            }
            builder.addIfNotNull(DisplayData.item("filePrefix", prefixString).withLabel("Output File Prefix"))
                    .addIfNotNull(DisplayData.item("fileSuffix", getFilenameSuffix()).withLabel("Output File Suffix"))
                    .addIfNotNull(DisplayData.item("shardNameTemplate", getShardTemplate()).withLabel("Output Shard Name Template"))
                    .addIfNotDefault(DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
                    //TODO: check this toString
                    .addIfNotNull(DisplayData.item("fileHeader", String.valueOf(getHeader())).withLabel("File Header"))
                    .addIfNotNull(DisplayData.item("fileFooter", String.valueOf(getFooter())).withLabel("File Footer"))
                    .add(DisplayData.item("writableByteChannelFactory", getWritableByteChannelFactory().toString())
                            .withLabel("Compression/Transformation Type"));
        }

        @Override
        protected Coder<Void> getDefaultOutputCoder() {
            return VoidCoder.of();
        }
    }

    /**
     * Possible text file compression types.
     */
    public enum CompressionType {
        /**
         * Automatically determine the compression type based on filename extension.
         */
        AUTO(""),
        /**
         * Uncompressed (i.e., may be split).
         */
        UNCOMPRESSED(""),
        /**
         * GZipped.
         */
        GZIP(".gz"),
        /**
         * BZipped.
         */
        BZIP2(".bz2"),
        /**
         * Zipped.
         */
        ZIP(".zip"),
        /**
         * Deflate compressed.
         */
        DEFLATE(".deflate");

        private String filenameSuffix;

        CompressionType(String suffix) {
            this.filenameSuffix = suffix;
        }

        /**
         * Determine if a given filename matches a compression type based on its extension.
         *
         * @param filename the filename to match
         * @return true iff the filename ends with the compression type's known extension.
         */
        public boolean matches(String filename) {
            return filename.toLowerCase().endsWith(filenameSuffix.toLowerCase());
        }
    }

    //////////////////////////////////////////////////////////////////////////////

    /**
     * Disable construction of utility class.
     */
    private CsvIO() {
    }
}
