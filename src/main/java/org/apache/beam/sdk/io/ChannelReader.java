package org.apache.beam.sdk.io;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;

public class ChannelReader extends Reader {
    private static final int READ_BUFFER_SIZE = 8 * 1024;
    private final ReadableByteChannel channel;
    private ByteBuffer readByteBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);

    public ChannelReader(ReadableByteChannel channel) {
        this.channel = channel;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        boolean eof = channel.read(readByteBuffer) == -1;
        if (eof) {
            return -1;
        }
        int charactersRead = readByteBuffer.position();

        String str = new String(readByteBuffer.array(), Charsets.UTF_8);
        for (int i = 0; i < str.length(); i++) {
            cbuf[i] = str.charAt(i);
        }
        return charactersRead;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
