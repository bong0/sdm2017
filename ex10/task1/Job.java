import frm.*;

import java.io.IOException;

public class Job {
    public static void main(String[] args) throws IOException {
        run("visits_small.csv", "favorites_small.json");
    }

    public static void run(String visitDatasetFileName, String outFileName) throws IOException
    {
        /*
        Read all the data
         */

        //Context<...> readerContext = new Context<>();
        //Reader reader = ...;
        //reader.read(..., ..., readerContext);

        /*
        Perform mapping and reducing steps (as many as needed)
         */

        //Context<...> mapper1Context = new Context<>();
        //Mapper<...> mapper1 = new ...;
        //mapper1.run(readerContext, mapper1Context);

        //Context<...> reducer1Context = new Context<>();
        //Reducer<...> reducer1 = new ...;
        //reducer1.run(mapper1Context, reducer1Context);

        //...

        /*
        Write resulting records to file
         */

        //RecordWriter<String, List<String>> writer = new ...;
        //writer.run(...);
    }
}
