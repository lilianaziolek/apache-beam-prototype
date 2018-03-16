package org.apache.beam.sdk.io;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.commons.csv.CSVRecord;

import java.io.*;

public class CsvCoder extends AtomicCoder<CSVRecord> {

    private static final CsvCoder INSTANCE = new CsvCoder();

    public static CsvCoder of() {
        return INSTANCE;
    }

    @Override
    public void encode(CSVRecord value, OutputStream outStream) throws CoderException, IOException {
        ObjectOutputStream oos = new ObjectOutputStream(outStream);
        oos.writeObject(value);
        oos.flush();
    }

    @Override
    public CSVRecord decode(InputStream inStream) throws CoderException, IOException {
        try {
            ObjectInputStream ois = new ObjectInputStream(inStream);
            return (CSVRecord) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not read object", e);
        }
    }
}
