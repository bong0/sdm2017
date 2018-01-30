package frm;

import java.io.File;
import java.io.IOException;

public abstract class Reader<KEYIN, VALUEIN> {
    /**
     *
     * @param input
     * @param key
     * @param context
     */
    public abstract void read(File input, KEYIN key, Context<KEYIN, VALUEIN> context) throws IOException;
}
