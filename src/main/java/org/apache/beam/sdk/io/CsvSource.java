package org.apache.beam.sdk.io;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.commons.csv.CSVRecord;

import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation detail of {@link CsvIO.Read}.
 * <p>
 * <p>A {@link FileBasedSource} which can decode CSV records.
 * <p>
 */
@VisibleForTesting
class CsvSource extends FileBasedSource<CSVRecord> {

    protected CsvSource(ValueProvider<String> fileOrPatternSpec, EmptyMatchTreatment emptyMatchTreatment) {
        super(fileOrPatternSpec, emptyMatchTreatment, 0);
    }

    protected CsvSource(ValueProvider<String> fileOrPatternSpec) {
        super(fileOrPatternSpec, 0);
    }

    protected CsvSource(MatchResult.Metadata fileMetadata, long minBundleSize, long startOffset, long endOffset) {
        super(fileMetadata, minBundleSize, startOffset, endOffset);
    }

    @Override
    protected boolean isSplittable() {
        return false;
    }

    @Override
    protected FileBasedSource<CSVRecord> createForSubrangeOfFile(MatchResult.Metadata fileMetadata, long start, long end) {
        return new CsvSource(fileMetadata, 0, start, end);
    }

    @Override
    protected CsvReader createSingleFileReader(PipelineOptions options) {
        return new CsvReader(this);
    }

    @Override
    public Coder<CSVRecord> getOutputCoder() {
        return CsvCoder.of();
    }

}
