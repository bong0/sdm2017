package frm;

/**
 * Abstract implementation for MAPPER
 * @param <KEYIN> type of incoming keys
 * @param <VALUEIN> type of incoming values
 * @param <KEYOUT> type of outgoing keys
 * @param <VALUEOUT> type of outgoing values
 */
public abstract class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    /**
     * Mapping function: (k, v) -> (k', v')*
     * @param key incoming key
     * @param value incoming value
     * @param context context (to store outgoing key-value-pairs
     */
    public abstract void map(KEYIN key, VALUEIN value, Context<KEYOUT, VALUEOUT> context);

    /**
     * Run mapper for the whole given context
     * @param contextIn incoming data
     * @param contextOut outgoing data
     */
    public void run(Context<KEYIN, VALUEIN> contextIn, Context<KEYOUT, VALUEOUT> contextOut)
    {
        for (KEYIN key: contextIn.keys) {
            for(VALUEIN value: contextIn.keyValues.get(key))
                map(key, value, contextOut);
        }
    }
}
