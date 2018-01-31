import frm.*;
import frm.Reader;

import java.io.*;

public class UserReader<KEYIN, VALUEIN> extends Reader<KEYIN, VALUEIN> {
    /**
     *
     * @param input
     * @param key
     * @param context
     */

    public static String MAGICQOUTE = "@#@";

    @SuppressWarnings("unchecked")
    public void read(File input, KEYIN key, Context<KEYIN, VALUEIN> context) throws IOException {

        BufferedReader reader = null;
        String line = "";

        try {
            reader = new BufferedReader(new FileReader(input));
            reader.readLine(); // throw first line away
            while ((line = reader.readLine()) != null) {
                String[] tuple = line.split(";");
                VALUEIN user = (VALUEIN)(MAGICQOUTE+tuple[1]+MAGICQOUTE); // put into magic /quotes/ to distinguish from URL
                KEYIN deviceId = (KEYIN)tuple[0];
                context.write(deviceId,user);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}