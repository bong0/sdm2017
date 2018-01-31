import frm.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Job {
    public static void main(String[] args) throws IOException {
        run("visits_big.csv", "favorites_big.json");
    }

    @SuppressWarnings("unchecked")
    public static void run(String visitDatasetFileName, String outFileName) throws IOException
    {
        /*
        Read all the data
         */

        Context<String,String> readerContext = new Context<String,String>();
        Reader usrReader = new UserReader<String, String>();
        // why is the devices csv not given as parameter in the codebase??
        usrReader.read(new File("devices.csv"), null, readerContext);  // I don't need the key since I implement two Readers for each filetype;

        Reader urlReader = new UrlReader<>();
        urlReader.read(new File(visitDatasetFileName), null, readerContext); // I don't need the key since I implement two Readers for each filetype;

        // readerContext is now filled, first (identity) mapper is not needed since there are already K/V tuples;

        /*
        Perform mapping and reducing steps (as many as needed)
         */

        // The map step is not needed here since the context already functions as an identity mapper
        // The input key is "deviceID", the buckets are grouped by this
        Context<String,String> detUserReducerOutputContext = new Context<>();
        Reducer<String,String,String,String> detUserReducer = new DetermineUserReducer();
        detUserReducer.run(readerContext, detUserReducerOutputContext);

        // The map step is not needed here since the context already functions as an identity mapper
        // The new key is "username", the buckets are grouped by this
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
