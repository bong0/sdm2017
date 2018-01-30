package frm;

/**
 * Abstract implementation for REDUCER
 * @param <KEYIN> type of incoming keys
 * @param <VALUEIN> type of incoming values
 * @param <KEYOUT> type of outgoing keys
 * @param <VALUEOUT> type of outgoing values
 */
public abstract class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    /**
     * Reducin function: (k, v..) -> (k', v')
     * @param key incoming key
     * @param value iterable/list of incoming values
     * @param context context (to store outgoing key-value-pairs
     */
    public abstract void reduce(KEYIN key, Iterable<VALUEIN> value, Context<KEYOUT, VALUEOUT> context);

    /**
     * Run reducer for the whole given context
     * @param contextIn incoming data
     * @param contextOut outgoing data
     */
    public void run(Context<KEYIN, VALUEIN> contextIn, Context<KEYOUT, VALUEOUT> contextOut)
    {
        for (KEYIN key: contextIn.keys) {
            reduce(key, contextIn.keyValues.get(key), contextOut);
        }
    }
}
