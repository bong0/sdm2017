import frm.*;
import frm.Reader;

import java.io.*;

@SuppressWarnings("unchecked")
public class UrlReader<KEYIN, VALUEIN> extends Reader<KEYIN, VALUEIN> {
    /**
     *
     * @param input
     * @param key
     * @param context
     */

    public void read(File input, KEYIN key, Context<KEYIN, VALUEIN> context) throws IOException {

        BufferedReader reader = null;
        String line = "";

        try {
            reader = new BufferedReader(new FileReader(input));
            reader.readLine(); // throw first line away

            while ((line = reader.readLine()) != null) {

                String[] tuple = line.split(";");
                VALUEIN url = (VALUEIN)tuple[1]; // put into magic /qoutes/ to distinguish from URL
                KEYIN deviceId = (KEYIN)tuple[0];
                context.write(deviceId,url);
            }

        } catch (FileNotFoundException e) {
            System.err.println("File not found! Please make sure CWD is the data directory or contains the csv files");
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