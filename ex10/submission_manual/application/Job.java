import frm.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Job {
    public static void main(String[] args) throws IOException {
        run("visits_small.csv", "favorites_small.json");
    }

    @SuppressWarnings("unchecked")
    public static void run(String visitDatasetFileName, String outFileName) throws IOException
    {
        /*
        Read all the data
         */

        Context<String,String> readerContext = new Context<String,String>();
        Reader usrReader = new UserReader<String, String>();
        // why is the devices csv not given as parameter??
        usrReader.read(new File("devices.csv"), null, readerContext);  // ?? What is the key field for even? We don't know it before reading the file

        Reader urlReader = new UrlReader<>();
        urlReader.read(new File(visitDatasetFileName), null, readerContext);  // ?? What is the key field for even? We don't know it before reading the file

        // readerContext is now filled, first (identity) mapper is not needed since there are already K/V tuples;

        /*
        Perform mapping and reducing steps (as many as needed)
         */

        Context<String,String> detUserReducerOutputContext = new Context<>();
        Reducer<String,String,String,String> detUserReducer = new DetermineUserReducer();
        detUserReducer.run(readerContext, detUserReducerOutputContext);

        Context<String,String> urlSortingReducerOutputContext = new Context<>();
        Reducer<String,String,String,String> urlSortingReducer = new UrlSortingReducer<>();
        urlSortingReducer.run(detUserReducerOutputContext, urlSortingReducerOutputContext);

        /*
        Write resulting records to file
         */

        RecordWriter<String,String> writer = new JsonRecordWriter<String,String>(new File(outFileName));
        writer.run(urlSortingReducerOutputContext);
    }
}
