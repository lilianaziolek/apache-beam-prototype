package org.apache.beam.sdk.io;

import org.apache.beam.runners.direct.repackaged.javax.annotation.Nullable;
import org.apache.beam.sdk.repackaged.com.google.protobuf.ByteString;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.NoSuchElementException;

import static org.apache.beam.sdks.java.extensions.protobuf.repackaged.com.google.common.base.Preconditions.checkState;

public class AltCsvReader extends FileBasedSource.FileBasedReader<CSVRecord> {

    private static final int READ_BUFFER_SIZE = 8192;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    private ByteString buffer;
    private int startOfDelimiterInBuffer;
    private int endOfDelimiterInBuffer;
    private long startOfRecord;
    private volatile long startOfNextRecord;
    private volatile boolean eof;
    private volatile boolean elementIsPresent;
    private @Nullable
    CSVRecord currentValue;
    private @Nullable
    ReadableByteChannel inChannel;
    private @Nullable
    byte[] delimiter;

    private AltCsvReader(CsvSource source, byte[] delimiter) {
        super(source);
        buffer = ByteString.EMPTY;
        this.delimiter = delimiter;
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
        if (!elementIsPresent) {
            throw new NoSuchElementException();
        }
        return startOfRecord;
    }

    @Override
    public long getSplitPointsRemaining() {
        if (isStarted() && startOfNextRecord >= getCurrentSource().getEndOffset()) {
            return isDone() ? 0 : 1;
        }
        return super.getSplitPointsRemaining();
    }

    @Override
    public CSVRecord getCurrent() throws NoSuchElementException {
        if (!elementIsPresent) {
            throw new NoSuchElementException();
        }
        return currentValue;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
        this.inChannel = channel;
        // If the first offset is greater than zero, we need to skip bytes until we see our
        // first delimiter.
        long startOffset = getCurrentSource().getStartOffset();
        if (startOffset > 0) {
            checkState(channel instanceof SeekableByteChannel,
                    "%s only supports reading from a SeekableByteChannel when given a start offset" + " greater than 0.",
                    CsvSource.class.getSimpleName());
            long requiredPosition = startOffset - 1;
            if (delimiter != null && startOffset >= delimiter.length) {
                // we need to move back the offset of at worse delimiter.size to be sure to see
                // all the bytes of the delimiter in the call to findDelimiterBounds() below
                requiredPosition = startOffset - delimiter.length;
            }
            ((SeekableByteChannel) channel).position(requiredPosition);
            findDelimiterBounds();
            buffer = buffer.substring(endOfDelimiterInBuffer);
            startOfNextRecord = requiredPosition + endOfDelimiterInBuffer;
            endOfDelimiterInBuffer = 0;
            startOfDelimiterInBuffer = 0;
        }
    }

    /**
     * Locates the start position and end position of the next delimiter. Will
     * consume the channel till either EOF or the delimiter bounds are found.
     * <p>
     * <p>This fills the buffer and updates the positions as follows:
     * <pre>{@code
     * ------------------------------------------------------
     * | element bytes | delimiter bytes | unconsumed bytes |
     * ------------------------------------------------------
     * 0            start of          end of              buffer
     *              delimiter         delimiter           size
     *              in buffer         in buffer
     * }</pre>
     */
    private void findDelimiterBounds() throws IOException {
        int bytePositionInBuffer = 0;
        while (true) {
            if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
                startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
                break;
            }

            byte currentByte = buffer.byteAt(bytePositionInBuffer);

            if (delimiter == null) {
                // default delimiter
                if (currentByte == '\n') {
                    startOfDelimiterInBuffer = bytePositionInBuffer;
                    endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;
                    break;
                } else if (currentByte == '\r') {
                    startOfDelimiterInBuffer = bytePositionInBuffer;
                    endOfDelimiterInBuffer = startOfDelimiterInBuffer + 1;

                    if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 2)) {
                        currentByte = buffer.byteAt(bytePositionInBuffer + 1);
                        if (currentByte == '\n') {
                            endOfDelimiterInBuffer += 1;
                        }
                    }
                    break;
                }
            } else {
                // user defined delimiter
                int i = 0;
                // initialize delimiter not found
                startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
                while ((i <= delimiter.length - 1) && (currentByte == delimiter[i])) {
                    // read next byte
                    i++;
                    if (tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + i + 1)) {
                        currentByte = buffer.byteAt(bytePositionInBuffer + i);
                    } else {
                        // corner case: delimiter truncated at the end of the file
                        startOfDelimiterInBuffer = endOfDelimiterInBuffer = bytePositionInBuffer;
                        break;
                    }
                }
                if (i == delimiter.length) {
                    // all bytes of delimiter found
                    endOfDelimiterInBuffer = bytePositionInBuffer + i;
                    break;
                }
            }
            // Move to the next byte in buffer.
            bytePositionInBuffer += 1;
        }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
        startOfRecord = startOfNextRecord;
        findDelimiterBounds();

        // If we have reached EOF file and consumed all of the buffer then we know
        // that there are no more records.
        if (eof && buffer.size() == 0) {
            elementIsPresent = false;
            return false;
        }

        decodeCurrentElement();
        startOfNextRecord = startOfRecord + endOfDelimiterInBuffer;
        return true;
    }

    /**
     * Decodes the current element updating the buffer to only contain the unconsumed bytes.
     * <p>
     * <p>This invalidates the currently stored {@code startOfDelimiterInBuffer} and
     * {@code endOfDelimiterInBuffer}.
     */
    private void decodeCurrentElement() throws IOException {
        ByteString dataToDecode = buffer.substring(0, startOfDelimiterInBuffer);
//        currentValue = dataToDecode.toStringUtf8();
        elementIsPresent = true;
        buffer = buffer.substring(endOfDelimiterInBuffer);
    }

    /**
     * Returns false if we were unable to ensure the minimum capacity by consuming the channel.
     */
    private boolean tryToEnsureNumberOfBytesInBuffer(int minCapacity) throws IOException {
        // While we aren't at EOF or haven't fulfilled the minimum buffer capacity,
        // attempt to read more bytes.
        while (buffer.size() <= minCapacity && !eof) {
            eof = inChannel.read(readBuffer) == -1;
            readBuffer.flip();
            buffer = buffer.concat(ByteString.copyFrom(readBuffer));
            readBuffer.clear();
        }
        // Return true if we were able to honor the minimum buffer capacity request
        return buffer.size() >= minCapacity;
    }
}
