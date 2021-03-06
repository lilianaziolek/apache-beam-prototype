package org.apache.beam.sdk.io;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;
import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;

/**
 * {@link PTransform}s for reading and writing text files.
 * <p>
 * <h2>Reading text files</h2>
 * <p>
 * <p>To read a {@link PCollection} from one or more text files, use {@code Csv2IO.read()} to
 * instantiate a transform and use {@link CsvIO.Read#from(String)} to specify the path of the
 * file(s) to be read. Alternatively, if the filenames to be read are themselves in a {@link
 * PCollection}, apply {@link CsvIO#readAll()} or {@link CsvIO#readFiles}.
 * <p>
 * <p>{@link #read} returns a {@link PCollection} of {@link CSVRecord CsvRecords}, each corresponding to
 * correctly parsed entry of an input UTF-8 text file.
 * <p>
 * <h3>Filepattern expansion and watching</h3>
 * <p>
 * <p>By default, the filepatterns are expanded only once. {@link Read#watchForNewFiles} and {@link
 * ReadAll#watchForNewFiles} allow streaming of new files matching the filepattern(s).
 * <p>
 * <p>By default, {@link #read} prohibits filepatterns that match no files, and {@link #readAll}
 * allows them in case the filepattern contains a glob wildcard character. Use {@link
 * CsvIO.Read#withEmptyMatchTreatment} and {@link CsvIO.ReadAll#withEmptyMatchTreatment} to
 * configure this behavior.
 * <p>
 * <p>Example 1: reading a file or filepattern.
 * <p>
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A simple Read of a local file (only runs locally):
 * PCollection<String> lines = p.apply(Csv2IO.read().from("/local/path/to/file.txt"));
 * }</pre>
 * <p>
 * <p>Example 2: reading a PCollection of filenames.
 * <p>
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // E.g. the filenames might be computed from other data in the pipeline, or
 * // read from a data source.
 * PCollection<String> filenames = ...;
 *
 * // Read all files in the collection.
 * PCollection<String> lines = filenames.apply(Csv2IO.readAll());
 * }</pre>
 * <p>
 * <p>Example 3: streaming new files matching a filepattern.
 * <p>
 * <pre>{@code
 * Pipeline p = ...;
 *
 * PCollection<String> lines = p.apply(Csv2IO.read()
 *     .from("/local/path/to/files/*")
 *     .watchForNewFiles(
 *       // Check for new files every minute
 *       Duration.standardMinutes(1),
 *       // Stop watching the filepattern if no new files appear within an hour
 *       afterTimeSinceNewOutput(Duration.standardHours(1))));
 * }</pre>
 * <p>
 * <h3>Reading a very large number of files</h3>
 * <p>
 * <p>If it is known that the filepattern will match a very large number of files (e.g. tens of
 * thousands or more), use {@link Read#withHintMatchesManyFiles} for better performance and
 * scalability. Note that it may decrease performance if the filepattern matches only a small number
 * of files.
 * <p>
 * <h2>Writing text files</h2>
 * <p>
 * <p>To write a {@link PCollection} to one or more text files, use {@code Csv2IO.write()}, using
 * {@link CsvIO.Write#to(String)} to specify the output prefix of the files to write.
 * <p>
 * <p>For example:
 * <p>
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<String> lines = ...;
 * lines.apply(Csv2IO.write().to("/path/to/file.txt"));
 *
 * // Same as above, only with Gzip compression:
 * PCollection<String> lines = ...;
 * lines.apply(Csv2IO.write().to("/path/to/file.txt"))
 *      .withSuffix(".txt")
 *      .withCompression(Compression.GZIP));
 * }</pre>
 * <p>
 * <p>Any existing files with the same names as generated output files will be overwritten.
 * <p>
 * <p>If you want better control over how filenames are generated than the default policy allows, a
 * custom {@link FilenamePolicy} can also be set using {@link CsvIO.Write#to(FilenamePolicy)}.
 * <p>
 * <h3>Advanced features</h3>
 * <p>
 * <p>{@link CsvIO} supports all features of {@link FileIO#write} and {@link FileIO#writeDynamic},
 * such as writing windowed/unbounded data, writing data to multiple destinations, and so on, by
 * providing a {@link Sink} via {@link #sink()}.
 * <p>
 * <p>For example, to write events of different type to different filenames:
 * <p>
 * <pre>{@code
 *   PCollection<Event> events = ...;
 *   events.apply(FileIO.<EventType, Event>writeDynamic()
 *         .by(Event::getType)
 *         .via(Csv2IO.sink(), Event::toString)
 *         .to(type -> nameFilesUsingWindowPaneAndShard(".../events/" + type + "/data", ".txt")));
 * }</pre>
 * <p>
 * <p>For backwards compatibility, {@link CsvIO} also supports the legacy
 * {@link DynamicDestinations} interface for advanced features via {@link
 * Write#to(DynamicDestinations)}.
 */
public class CsvIO {
    /**
     * A {@link PTransform} that reads from one or more text files and returns a bounded
     * {@link PCollection} containing one element for each line of the input files.
     */
    public static Read read() {
        return new AutoValue_CsvIO_Read.Builder().setCompression(Compression.AUTO)
                .setHintMatchesManyFiles(false)
                .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
                .build();
    }

    /**
     * A {@link PTransform} that works like {@link #read}, but reads each file in a {@link
     * PCollection} of filepatterns.
     * <p>
     * <p>Can be applied to both bounded and unbounded {@link PCollection PCollections}, so this is
     * suitable for reading a {@link PCollection} of filepatterns arriving as a stream. However, every
     * filepattern is expanded once at the moment it is processed, rather than watched for new files
     * matching the filepattern to appear. Likewise, every file is read once, rather than watched for
     * new entries.
     */
    public static ReadAll readAll() {
        return new AutoValue_CsvIO_ReadAll.Builder().setCompression(Compression.AUTO)
                .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
                .build();
    }

    /**
     * Like {@link #read}, but reads each file in a {@link PCollection} of {@link
     * FileIO.ReadableFile}, returned by {@link FileIO#readMatches}.
     */
    public static ReadFiles readFiles() {
        return new AutoValue_CsvIO_ReadFiles.Builder()
                // 64MB is a reasonable value that allows to amortize the cost of opening files,
                // but is not so large as to exhaust a typical runner's maximum amount of output per
                // ProcessElement call.
                .setDesiredBundleSizeBytes(64 * 1024 * 1024L).build();
    }

    /**
     * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
     * matching a sharding pattern), with each element of the input collection encoded into its own
     * line.
     */
    public static Write write() {
        return new CsvIO.Write();
    }

    /**
     * A {@link PTransform} that writes a {@link PCollection} to a text file (or multiple text files
     * matching a sharding pattern), with each element of the input collection encoded into its own
     * line.
     * <p>
     * <p>This version allows you to apply {@link CsvIO} writes to a PCollection of a custom type
     * {@link UserT}. A format mechanism that converts the input type {@link UserT} to the String that
     * will be written to the file must be specified. If using a custom {@link DynamicDestinations}
     * object this is done using {@link DynamicDestinations#formatRecord}, otherwise the {@link
     * TypedWrite#withFormatFunction} can be used to specify a format function.
     * <p>
     * <p>The advantage of using a custom type is that is it allows a user-provided {@link
     * DynamicDestinations} object, set via {@link Write#to(DynamicDestinations)} to examine the
     * custom type when choosing a destination.
     */
    public static <UserT> TypedWrite<UserT, Void> writeCustomType() {
        return new AutoValue_CsvIO_TypedWrite.Builder<UserT, Void>().setFilenamePrefix(null)
                .setTempDirectory(null)
                .setShardTemplate(null)
                .setFilenameSuffix(null)
                .setFilenamePolicy(null)
                .setDynamicDestinations(null)
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

        abstract MatchConfiguration getMatchConfiguration();

        abstract boolean getHintMatchesManyFiles();

        abstract Compression getCompression();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setFilepattern(ValueProvider<String> filepattern);

            abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

            abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);

            abstract Builder setCompression(Compression compression);

            abstract Read build();
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
         * <p>
         * <p>If it is known that the filepattern will match a very large number of files (at least tens
         * of thousands), use {@link #withHintMatchesManyFiles} for better performance and scalability.
         */
        public Read from(String filepattern) {
            checkArgument(filepattern != null, "filepattern can not be null");
            return from(StaticValueProvider.of(filepattern));
        }

        /**
         * Same as {@code from(filepattern)}, but accepting a {@link ValueProvider}.
         */
        public Read from(ValueProvider<String> filepattern) {
            checkArgument(filepattern != null, "filepattern can not be null");
            return toBuilder().setFilepattern(filepattern).build();
        }

        /**
         * Sets the {@link MatchConfiguration}.
         */
        public Read withMatchConfiguration(MatchConfiguration matchConfiguration) {
            return toBuilder().setMatchConfiguration(matchConfiguration).build();
        }

        /**
         * @deprecated Use {@link #withCompression}.
         */
        @Deprecated
        public Read withCompressionType(CsvIO.CompressionType compressionType) {
            return withCompression(compressionType.canonical);
        }

        /**
         * Reads from input sources using the specified compression type.
         * <p>
         * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
         */
        public Read withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        /**
         * See {@link MatchConfiguration#continuously}.
         * <p>
         * <p>This works only in runners supporting {@link Kind#SPLITTABLE_DO_FN}.
         */
        @Experimental(Kind.SPLITTABLE_DO_FN)
        public Read watchForNewFiles(Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
            return withMatchConfiguration(getMatchConfiguration().continuously(pollInterval, terminationCondition));
        }

        /**
         * Hints that the filepattern specified in {@link #from(String)} matches a very large number of
         * files.
         * <p>
         * <p>This hint may cause a runner to execute the transform differently, in a way that improves
         * performance for this case, but it may worsen performance if the filepattern matches only
         * a small number of files (e.g., in a runner that supports dynamic work rebalancing, it will
         * happen less efficiently within individual files).
         */
        public Read withHintMatchesManyFiles() {
            return toBuilder().setHintMatchesManyFiles(true).build();
        }

        /**
         * See {@link MatchConfiguration#withEmptyMatchTreatment}.
         */
        public Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
            return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
        }

        static boolean isSelfOverlapping(byte[] s) {
            // s self-overlaps if v exists such as s = vu = wv with u and w non empty
            for (int i = 1; i < s.length - 1; ++i) {
                if (ByteBuffer.wrap(s, 0, i).equals(ByteBuffer.wrap(s, s.length - i, i))) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public PCollection<CSVRecord> expand(PBegin input) {
            checkNotNull(getFilepattern(), "need to set the filepattern of a Csv2IO.Read transform");
            if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
                return input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
            }
            // All other cases go through ReadAll.
            return input.apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
                    .apply("Via ReadAll", readAll().withCompression(getCompression()).withMatchConfiguration(getMatchConfiguration()));
        }

        // Helper to create a source specific to the requested compression type.
        protected FileBasedSource<CSVRecord> getSource() {
            return CompressedSource.from(new CsvSource(getFilepattern(), getMatchConfiguration().getEmptyMatchTreatment()))
                    .withCompression(getCompression());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder.add(DisplayData.item("compressionType", getCompression().toString()).withLabel("Compression Type"))
                    .addIfNotNull(DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"))
                    .include("matchConfiguration", getMatchConfiguration());
        }
    }

    /////////////////////////////////////////////////////////////////////////////

    /**
     * Implementation of {@link #readAll}.
     */
    @AutoValue
    public abstract static class ReadAll extends PTransform<PCollection<String>, PCollection<CSVRecord>> {
        abstract MatchConfiguration getMatchConfiguration();

        abstract Compression getCompression();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

            abstract Builder setCompression(Compression compression);

            abstract ReadAll build();
        }

        /**
         * Sets the {@link MatchConfiguration}.
         */
        public ReadAll withMatchConfiguration(MatchConfiguration configuration) {
            return toBuilder().setMatchConfiguration(configuration).build();
        }

        /**
         * @deprecated Use {@link #withCompression}.
         */
        @Deprecated
        public ReadAll withCompressionType(CsvIO.CompressionType compressionType) {
            return withCompression(compressionType.canonical);
        }

        /**
         * Reads from input sources using the specified compression type.
         * <p>
         * <p>If no compression type is specified, the default is {@link Compression#AUTO}.
         */
        public ReadAll withCompression(Compression compression) {
            return toBuilder().setCompression(compression).build();
        }

        /**
         * Same as {@link Read#withEmptyMatchTreatment}.
         */
        public ReadAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
            return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
        }

        /**
         * Same as {@link Read#watchForNewFiles(Duration, TerminationCondition)}.
         */
        @Experimental(Kind.SPLITTABLE_DO_FN)
        public ReadAll watchForNewFiles(Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
            return withMatchConfiguration(getMatchConfiguration().continuously(pollInterval, terminationCondition));
        }

        @Override
        public PCollection<CSVRecord> expand(PCollection<String> input) {
            return input.apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
                    .apply(FileIO.readMatches().withCompression(getCompression()).withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
                    .apply(readFiles());
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);

            builder.add(DisplayData.item("compressionType", getCompression().toString()).withLabel("Compression Type"))
                    .include("matchConfiguration", getMatchConfiguration());
        }

    }

    /**
     * Implementation of {@link #readFiles}.
     */
    @AutoValue
    public abstract static class ReadFiles extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<CSVRecord>> {
        abstract long getDesiredBundleSizeBytes();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

            abstract ReadFiles build();
        }

        @VisibleForTesting
        ReadFiles withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
            return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
        }


        @Override
        public PCollection<CSVRecord> expand(PCollection<FileIO.ReadableFile> input) {
            return input.apply("Read all via FileBasedSource",
                    new ReadAllViaFileBasedSource<>(getDesiredBundleSizeBytes(), new CreateCsvSourceFn(), CsvCoder.of()));
        }

        private static class CreateCsvSourceFn implements SerializableFunction<String, FileBasedSource<CSVRecord>> {

            private CreateCsvSourceFn() {
            }

            @Override
            public FileBasedSource<CSVRecord> apply(String input) {
                return new CsvSource(StaticValueProvider.of(input), EmptyMatchTreatment.DISALLOW);
            }
        }
    }

    // ///////////////////////////////////////////////////////////////////////////

    /**
     * Implementation of {@link #write}.
     */
    @AutoValue
    public abstract static class TypedWrite<UserT, DestinationT> extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
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
         * The base directory used for generating temporary files.
         */
        @Nullable
        abstract ValueProvider<ResourceId> getTempDirectory();

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
        abstract FilenamePolicy getFilenamePolicy();

        /**
         * Allows for value-dependent {@link DynamicDestinations} to be vended.
         */
        @Nullable
        abstract DynamicDestinations<UserT, DestinationT, CSVRecord> getDynamicDestinations();

        /**
         * A destination function for using {@link DefaultFilenamePolicy}.
         */
        @Nullable
        abstract SerializableFunction<UserT, Params> getDestinationFunction();

        /**
         * A default destination for empty PCollections.
         */
        @Nullable
        abstract Params getEmptyDestination();

        /**
         * A function that converts UserT to a String, for writing to the file.
         */
        @Nullable
        abstract SerializableFunction<UserT, CSVRecord> getFormatFunction();

        /**
         * Whether to write windowed output files.
         */
        abstract boolean getWindowedWrites();

        /**
         * The {@link WritableByteChannelFactory} to be used by the {@link FileBasedSink}. Default is
         * {@link FileBasedSink.CompressionType#UNCOMPRESSED}.
         */
        abstract WritableByteChannelFactory getWritableByteChannelFactory();

        abstract Builder<UserT, DestinationT> toBuilder();

        @AutoValue.Builder
        abstract static class Builder<UserT, DestinationT> {
            abstract Builder<UserT, DestinationT> setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix);

            abstract Builder<UserT, DestinationT> setTempDirectory(@Nullable ValueProvider<ResourceId> tempDirectory);

            abstract Builder<UserT, DestinationT> setShardTemplate(@Nullable String shardTemplate);

            abstract Builder<UserT, DestinationT> setFilenameSuffix(@Nullable String filenameSuffix);

            abstract Builder<UserT, DestinationT> setHeader(@Nullable CSVRecord header);

            abstract Builder<UserT, DestinationT> setFooter(@Nullable CSVRecord footer);

            abstract Builder<UserT, DestinationT> setFilenamePolicy(@Nullable FilenamePolicy filenamePolicy);

            abstract Builder<UserT, DestinationT> setDynamicDestinations(
                    @Nullable DynamicDestinations<UserT, DestinationT, CSVRecord> dynamicDestinations);

            abstract Builder<UserT, DestinationT> setDestinationFunction(@Nullable SerializableFunction<UserT, Params> destinationFunction);

            abstract Builder<UserT, DestinationT> setEmptyDestination(Params emptyDestination);

            abstract Builder<UserT, DestinationT> setFormatFunction(@Nullable SerializableFunction<UserT, CSVRecord> formatFunction);

            abstract Builder<UserT, DestinationT> setNumShards(int numShards);

            abstract Builder<UserT, DestinationT> setWindowedWrites(boolean windowedWrites);

            abstract Builder<UserT, DestinationT> setWritableByteChannelFactory(WritableByteChannelFactory writableByteChannelFactory);

            abstract TypedWrite<UserT, DestinationT> build();
        }

        /**
         * Writes to text files with the given prefix. The given {@code prefix} can reference any {@link
         * FileSystem} on the classpath. This prefix is used by the {@link DefaultFilenamePolicy} to
         * generate filenames.
         * <p>
         * <p>By default, a {@link DefaultFilenamePolicy} will be used built using the specified prefix
         * to define the base output directory and file prefix, a shard identifier (see {@link
         * #withNumShards(int)}), and a common suffix (if supplied using {@link #withSuffix(String)}).
         * <p>
         * <p>This default policy can be overridden using {@link #to(FilenamePolicy)}, in which case
         * {@link #withShardNameTemplate(String)} and {@link #withSuffix(String)} should not be set.
         * Custom filename policies do not automatically see this prefix - you should explicitly pass
         * the prefix into your {@link FilenamePolicy} object if you need this.
         * <p>
         * <p>If {@link #withTempDirectory} has not been called, this filename prefix will be used to
         * infer a directory for temporary files.
         */
        public TypedWrite<UserT, DestinationT> to(String filenamePrefix) {
            return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
        }

        /**
         * Like {@link #to(String)}.
         */
        @Experimental(Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT> to(ResourceId filenamePrefix) {
            return toResource(StaticValueProvider.of(filenamePrefix));
        }

        /**
         * Like {@link #to(String)}.
         */
        public TypedWrite<UserT, DestinationT> to(ValueProvider<String> outputPrefix) {
            return toResource(NestedValueProvider.of(outputPrefix, FileBasedSink::convertToFileResourceIfPossible));
        }

        /**
         * Writes to files named according to the given {@link FilenamePolicy}. A
         * directory for temporary files must be specified using {@link #withTempDirectory}.
         */
        public TypedWrite<UserT, DestinationT> to(FilenamePolicy filenamePolicy) {
            return toBuilder().setFilenamePolicy(filenamePolicy).build();
        }

        /**
         * Use a {@link DynamicDestinations} object to vend {@link FilenamePolicy} objects. These
         * objects can examine the input record when creating a {@link FilenamePolicy}. A directory for
         * temporary files must be specified using {@link #withTempDirectory}.
         *
         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} with {@link #sink()}
         * instead.
         */
        @Deprecated
        public <NewDestinationT> TypedWrite<UserT, NewDestinationT> to(DynamicDestinations<UserT, NewDestinationT, CSVRecord> dynamicDestinations) {
            return (TypedWrite) toBuilder().setDynamicDestinations((DynamicDestinations) dynamicDestinations).build();
        }

        /**
         * Write to dynamic destinations using the default filename policy. The destinationFunction maps
         * the input record to a {@link Params} object that specifies where the
         * records should be written (base filename, file suffix, and shard template). The
         * emptyDestination parameter specified where empty files should be written for when the written
         * {@link PCollection} is empty.
         *
         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} with {@link #sink()}
         * instead.
         */
        @Deprecated
        public TypedWrite<UserT, Params> to(SerializableFunction<UserT, Params> destinationFunction, Params emptyDestination) {
            return (TypedWrite) toBuilder().setDestinationFunction(destinationFunction).setEmptyDestination(emptyDestination).build();
        }

        /**
         * Like {@link #to(ResourceId)}.
         */
        @Experimental(Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT> toResource(ValueProvider<ResourceId> filenamePrefix) {
            return toBuilder().setFilenamePrefix(filenamePrefix).build();
        }

        /**
         * Specifies a format function to convert {@link UserT} to the output type. If {@link
         * #to(DynamicDestinations)} is used, {@link DynamicDestinations#formatRecord(Object)} must be
         * used instead.
         *
         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} with {@link #sink()}
         * instead.
         */
        @Deprecated
        public TypedWrite<UserT, DestinationT> withFormatFunction(@Nullable SerializableFunction<UserT, CSVRecord> formatFunction) {
            return toBuilder().setFormatFunction(formatFunction).build();
        }

        /**
         * Set the base directory used to generate temporary files.
         */
        @Experimental(Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT> withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
            return toBuilder().setTempDirectory(tempDirectory).build();
        }

        /**
         * Set the base directory used to generate temporary files.
         */
        @Experimental(Kind.FILESYSTEM)
        public TypedWrite<UserT, DestinationT> withTempDirectory(ResourceId tempDirectory) {
            return withTempDirectory(StaticValueProvider.of(tempDirectory));
        }

        /**
         * Uses the given {@link ShardNameTemplate} for naming output files. This option may only be
         * used when using one of the default filename-prefix to() overrides - i.e. not when using
         * either {@link #to(FilenamePolicy)} or {@link #to(DynamicDestinations)}.
         * <p>
         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
         * used.
         */
        public TypedWrite<UserT, DestinationT> withShardNameTemplate(String shardTemplate) {
            return toBuilder().setShardTemplate(shardTemplate).build();
        }

        /**
         * Configures the filename suffix for written files. This option may only be used when using one
         * of the default filename-prefix to() overrides - i.e. not when using either {@link
         * #to(FilenamePolicy)} or {@link #to(DynamicDestinations)}.
         * <p>
         * <p>See {@link DefaultFilenamePolicy} for how the prefix, shard name template, and suffix are
         * used.
         */
        public TypedWrite<UserT, DestinationT> withSuffix(String filenameSuffix) {
            return toBuilder().setFilenameSuffix(filenameSuffix).build();
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
        public TypedWrite<UserT, DestinationT> withNumShards(int numShards) {
            checkArgument(numShards >= 0);
            return toBuilder().setNumShards(numShards).build();
        }

        /**
         * Forces a single file as output and empty shard name template.
         * <p>
         * <p>For unwindowed writes, constraining the number of shards is likely to reduce the
         * performance of a pipeline. Setting this value is not recommended unless you require a
         * specific number of output files.
         * <p>
         * <p>This is equivalent to {@code .withNumShards(1).withShardNameTemplate("")}
         */
        public TypedWrite<UserT, DestinationT> withoutSharding() {
            return withNumShards(1).withShardNameTemplate("");
        }

        /**
         * Adds a header string to each file. A newline after the header is added automatically.
         * <p>
         * <p>A {@code null} value will clear any previously configured header.
         */
        public TypedWrite<UserT, DestinationT> withHeader(@Nullable CSVRecord header) {
            return toBuilder().setHeader(header).build();
        }

        /**
         * Adds a footer string to each file. A newline after the footer is added automatically.
         * <p>
         * <p>A {@code null} value will clear any previously configured footer.
         */
        public TypedWrite<UserT, DestinationT> withFooter(@Nullable CSVRecord footer) {
            return toBuilder().setFooter(footer).build();
        }

        /**
         * Returns a transform for writing to text files like this one but that has the given {@link
         * WritableByteChannelFactory} to be used by the {@link FileBasedSink} during output. The
         * default is value is {@link Compression#UNCOMPRESSED}.
         * <p>
         * <p>A {@code null} value will reset the value to the default value mentioned above.
         */
        public TypedWrite<UserT, DestinationT> withWritableByteChannelFactory(WritableByteChannelFactory writableByteChannelFactory) {
            return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
        }

        /**
         * Returns a transform for writing to text files like this one but that compresses output using
         * the given {@link Compression}. The default value is {@link Compression#UNCOMPRESSED}.
         */
        public TypedWrite<UserT, DestinationT> withCompression(Compression compression) {
            checkArgument(compression != null, "compression can not be null");
            return withWritableByteChannelFactory(FileBasedSink.CompressionType.fromCanonical(compression));
        }

        /**
         * Preserves windowing of input elements and writes them to files based on the element's window.
         * <p>
         * <p>If using {@link #to(FilenamePolicy)}. Filenames will be generated using
         * {@link FilenamePolicy#windowedFilename}. See also {@link WriteFiles#withWindowedWrites()}.
         */
        public TypedWrite<UserT, DestinationT> withWindowedWrites() {
            return toBuilder().setWindowedWrites(true).build();
        }

        private DynamicDestinations<UserT, DestinationT, CSVRecord> resolveDynamicDestinations() {
            DynamicDestinations<UserT, DestinationT, CSVRecord> dynamicDestinations = getDynamicDestinations();
            if (dynamicDestinations == null) {
                if (getDestinationFunction() != null) {
                    // In this case, DestinationT == Params
                    dynamicDestinations = (DynamicDestinations) DynamicFileDestinations.toDefaultPolicies(getDestinationFunction(),
                            getEmptyDestination(), getFormatFunction());
                } else {
                    // In this case, DestinationT == Void
                    FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
                    if (usedFilenamePolicy == null) {
                        usedFilenamePolicy = DefaultFilenamePolicy.fromStandardParameters(getFilenamePrefix(), getShardTemplate(),
                                getFilenameSuffix(), getWindowedWrites());
                    }
                    dynamicDestinations = (DynamicDestinations) DynamicFileDestinations.constant(usedFilenamePolicy, getFormatFunction());
                }
            }
            return dynamicDestinations;
        }

        @Override
        public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
            checkState(getFilenamePrefix() != null || getTempDirectory() != null,
                    "Need to set either the filename prefix or the tempDirectory of a Csv2IO.Write " + "transform.");

            List<?> allToArgs = Lists.newArrayList(getFilenamePolicy(), getDynamicDestinations(), getFilenamePrefix(), getDestinationFunction());
            checkArgument(1 == Iterables.size(allToArgs.stream().filter(Predicates.notNull()::apply).collect(Collectors.toList())),
                    "Exactly one of filename policy, dynamic destinations, filename prefix, or destination " + "function must be set");

            if (getDynamicDestinations() != null) {
                checkArgument(getFormatFunction() == null,
                        "A format function should not be specified " + "with DynamicDestinations. Use DynamicDestinations.formatRecord instead");
            }
            if (getFilenamePolicy() != null || getDynamicDestinations() != null) {
                checkState(getShardTemplate() == null && getFilenameSuffix() == null,
                        "shardTemplate and filenameSuffix should only be used with the default " + "filename policy");
            }
            ValueProvider<ResourceId> tempDirectory = getTempDirectory();
            if (tempDirectory == null) {
                tempDirectory = getFilenamePrefix();
            }
            WriteFiles<UserT, DestinationT, CSVRecord> write = WriteFiles.to(
                    new CsvSink<>(tempDirectory, resolveDynamicDestinations(), getHeader(), getFooter(), getWritableByteChannelFactory()));
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

            resolveDynamicDestinations().populateDisplayData(builder);
            builder.addIfNotDefault(DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"), 0)
                    .addIfNotNull(DisplayData.item("tempDirectory", getTempDirectory()).withLabel("Directory for temporary files"))
                    .addIfNotNull(DisplayData.item("fileHeader", String.valueOf(getHeader())).withLabel("File Header"))
                    .addIfNotNull(DisplayData.item("fileFooter", String.valueOf(getFooter())).withLabel("File Footer"))
                    .add(DisplayData.item("writableByteChannelFactory", getWritableByteChannelFactory().toString())
                            .withLabel("Compression/Transformation Type"));
        }
    }

    /**
     * This class is used as the default return value of {@link CsvIO#write()}.
     * <p>
     * <p>All methods in this class delegate to the appropriate method of {@link CsvIO.TypedWrite}.
     * This class exists for backwards compatibility, and will be removed in Beam 3.0.
     */
    public static class Write extends PTransform<PCollection<CSVRecord>, PDone> {
        @VisibleForTesting
        TypedWrite<CSVRecord, ?> inner;

        Write() {
            this(CsvIO.writeCustomType());
        }

        Write(TypedWrite<CSVRecord, ?> inner) {
            this.inner = inner;
        }

        /**
         * See {@link TypedWrite#to(String)}.
         */
        public Write to(String filenamePrefix) {
            return new Write(inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
        }

        /**
         * See {@link TypedWrite#to(ResourceId)}.
         */
        @Experimental(Kind.FILESYSTEM)
        public Write to(ResourceId filenamePrefix) {
            return new Write(inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
        }

        /**
         * See {@link TypedWrite#to(ValueProvider)}.
         */
        public Write to(ValueProvider<String> outputPrefix) {
            return new Write(inner.to(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
        }

        /**
         * See {@link TypedWrite#toResource(ValueProvider)}.
         */
        @Experimental(Kind.FILESYSTEM)
        public Write toResource(ValueProvider<ResourceId> filenamePrefix) {
            return new Write(inner.toResource(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
        }

        /**
         * See {@link TypedWrite#to(FilenamePolicy)}.
         */
        @Experimental(Kind.FILESYSTEM)
        public Write to(FilenamePolicy filenamePolicy) {
            return new Write(inner.to(filenamePolicy).withFormatFunction(SerializableFunctions.identity()));
        }

        /**
         * See {@link TypedWrite#to(DynamicDestinations)}.
         *
         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} ()} with {@link
         * #sink()} instead.
         */
        @Experimental(Kind.FILESYSTEM)
        @Deprecated
        public Write to(DynamicDestinations<String, ?, CSVRecord> dynamicDestinations) {
            return new Write(inner.to((DynamicDestinations) dynamicDestinations).withFormatFunction(null));
        }

        /**
         * See {@link TypedWrite#to(SerializableFunction, Params)}.
         *
         * @deprecated Use {@link FileIO#write()} or {@link FileIO#writeDynamic()} ()} with {@link
         * #sink()} instead.
         */
        @Experimental(Kind.FILESYSTEM)
        @Deprecated
        public Write to(SerializableFunction<CSVRecord, Params> destinationFunction, Params emptyDestination) {
            return new Write(inner.to(destinationFunction, emptyDestination).withFormatFunction(SerializableFunctions.identity()));
        }

        /**
         * See {@link TypedWrite#withTempDirectory(ValueProvider)}.
         */
        @Experimental(Kind.FILESYSTEM)
        public Write withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
            return new Write(inner.withTempDirectory(tempDirectory));
        }

        /**
         * See {@link TypedWrite#withTempDirectory(ResourceId)}.
         */
        @Experimental(Kind.FILESYSTEM)
        public Write withTempDirectory(ResourceId tempDirectory) {
            return new Write(inner.withTempDirectory(tempDirectory));
        }

        /**
         * See {@link TypedWrite#withShardNameTemplate(String)}.
         */
        public Write withShardNameTemplate(String shardTemplate) {
            return new Write(inner.withShardNameTemplate(shardTemplate));
        }

        /**
         * See {@link TypedWrite#withSuffix(String)}.
         */
        public Write withSuffix(String filenameSuffix) {
            return new Write(inner.withSuffix(filenameSuffix));
        }

        /**
         * See {@link TypedWrite#withNumShards(int)}.
         */
        public Write withNumShards(int numShards) {
            return new Write(inner.withNumShards(numShards));
        }

        /**
         * See {@link TypedWrite#withoutSharding()}.
         */
        public Write withoutSharding() {
            return new Write(inner.withoutSharding());
        }

        /**
         * See {@link TypedWrite#withHeader(String)}.
         */
        public Write withHeader(@Nullable CSVRecord header) {
            return new Write(inner.withHeader(header));
        }

        /**
         * See {@link TypedWrite#withFooter(String)}.
         */
        public Write withFooter(@Nullable CSVRecord footer) {
            return new Write(inner.withFooter(footer));
        }

        /**
         * See {@link TypedWrite#withWritableByteChannelFactory(WritableByteChannelFactory)}.
         */
        public Write withWritableByteChannelFactory(WritableByteChannelFactory writableByteChannelFactory) {
            return new Write(inner.withWritableByteChannelFactory(writableByteChannelFactory));
        }

        /**
         * See {@link TypedWrite#withWindowedWrites}.
         */
        public Write withWindowedWrites() {
            return new Write(inner.withWindowedWrites());
        }

        /**
         * Specify that output filenames are wanted.
         * <p>
         * <p>The nested {@link TypedWrite}transform always has access to output filenames, however due
         * to backwards-compatibility concerns, {@link Write} cannot return them. This method simply
         * returns the inner {@link TypedWrite} transform which has {@link WriteFilesResult} as its
         * output type, allowing access to output files.
         * <p>
         * <p>The supplied {@code DestinationT} type must be: the same as that supplied in {@link
         * #to(DynamicDestinations)} if that method was used; {@link Params} if {@link
         * #to(SerializableFunction, Params)} was used, or {@code Void} otherwise.
         */
        public <DestinationT> TypedWrite<String, DestinationT> withOutputFilenames() {
            return (TypedWrite) inner;
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            inner.populateDisplayData(builder);
        }

        @Override
        public PDone expand(PCollection<CSVRecord> input) {
            inner.expand(input);
            return PDone.in(input.getPipeline());
        }
    }

    /**
     * @deprecated Use {@link Compression}.
     */
    @Deprecated
    public enum CompressionType {
        /**
         * @see Compression#AUTO
         */
        AUTO(Compression.AUTO),

        /**
         * @see Compression#UNCOMPRESSED
         */
        UNCOMPRESSED(Compression.UNCOMPRESSED),

        /**
         * @see Compression#GZIP
         */
        GZIP(Compression.GZIP),

        /**
         * @see Compression#BZIP2
         */
        BZIP2(Compression.BZIP2),

        /**
         * @see Compression#ZIP
         */
        ZIP(Compression.ZIP),

        /**
         * @see Compression#ZIP
         */
        DEFLATE(Compression.DEFLATE);

        private Compression canonical;

        CompressionType(Compression canonical) {
            this.canonical = canonical;
        }

        /**
         * @see Compression#matches
         */
        public boolean matches(String filename) {
            return canonical.matches(filename);
        }
    }

    //////////////////////////////////////////////////////////////////////////////

    /**
     * Creates a {@link Sink} that writes newline-delimited strings in UTF-8, for use with {@link
     * FileIO#write}.
     */
    public static Sink sink() {
        return new AutoValue_CsvIO_Sink.Builder().build();
    }

    /**
     * Implementation of {@link #sink}.
     */
    @AutoValue
    public abstract static class Sink implements FileIO.Sink<CSVRecord> {
        @Nullable
        abstract String getHeader();

        @Nullable
        abstract String getFooter();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setHeader(String header);

            abstract Builder setFooter(String footer);

            abstract Sink build();
        }

        public Sink withHeader(String header) {
            checkArgument(header != null, "header can not be null");
            return toBuilder().setHeader(header).build();
        }

        public Sink withFooter(String footer) {
            checkArgument(footer != null, "footer can not be null");
            return toBuilder().setFooter(footer).build();
        }

        @Nullable
        private transient PrintWriter writer;

        @Override
        public void open(WritableByteChannel channel) throws IOException {
            writer = new PrintWriter(Channels.newOutputStream(channel));
            if (getHeader() != null) {
                writer.println(getHeader());
            }
        }

        @Override
        public void write(CSVRecord element) throws IOException {
            writer.println(element);
        }

        @Override
        public void flush() throws IOException {
            if (getFooter() != null) {
                writer.println(getFooter());
            }
            writer.close();
        }
    }

    /**
     * Disable construction of utility class.
     */
    private CsvIO() {
    }
}
