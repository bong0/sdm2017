package frm;

import java.io.File;
import java.io.IOException;

/**
 * RecordWriter: Write records/key-value-pairs to file
 * @param <K> type of keys
 * @param <V> type of values
 */
public abstract class RecordWriter<K, V> {
    File outFile;

    /**
     * Constructor
     * @param outFile file to write
     * @throws IOException
     */
    public RecordWriter(File outFile) throws IOException {
        this.outFile = outFile;
    }

    public void run(Context<K, V> context) throws IOException {
        start();
        for (K key: context.keys) {
            for(V value: context.keyValues.get(key))
                write(key, value);
        }
        finish();
    }

    /**
     * Write a single record
     * @param key key of the record
     * @param value value of the record
     * @throws IOException
     */
    public abstract void write(K key, V value) throws IOException;

    /**
     * Prepare writing
     * @throws IOException
     */
    public abstract void start() throws IOException;

    /**
     * Finalize writing
     * @throws IOException
     */
    public abstract void finish() throws IOException;
}
