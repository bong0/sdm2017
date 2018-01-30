import frm.RecordWriter;

import java.io.*;

public class JsonRecordWriter<K, V> extends RecordWriter<K,V> {
    File outFile;
    BufferedWriter bw;
    boolean firstValueWritten = false;

    public JsonRecordWriter(File outFile) throws IOException {
        super(outFile);
        bw = new BufferedWriter(new FileWriter(outFile));
    }

    /**
     * Write a single record
     * @param key key of the record
     * @param value value of the record
     * @throws IOException
     */
    /*
    Yes it is super ugly not to use a generator here but since javaSE brings none with it and I don't want any issues
    to happen with grading on your side because you don't have javaEE or whatever, I do it myself...
     */
    public void write(K key, V value) throws IOException {
        if(firstValueWritten){
            bw.write(",\n"); // write comma of the preceding object if we are in the middle of output
        } else {firstValueWritten = true;}

        bw.write("{");

            bw.write("\""+(String)key+"\":"); // write `"user":`
            bw.write("["); // array of urls start
            StringBuilder urlarray = new StringBuilder();
            for(String url : ((String)value).split(UrlSortingReducer.DELIMITER)){
                if(url=="") continue;
                urlarray.append("\"");
                urlarray.append(url);
                urlarray.append("\",");
            }
            urlarray.deleteCharAt(urlarray.length()-1); // remove last comma
            bw.write(urlarray.toString());
            bw.write("]"); // array of urls end

        bw.write("}");
    }

    /**
     * Prepare writing
     * @throws IOException
     */
    public void start() throws IOException {
        bw.write("[");
    }

    /**
     * Finalize writing
     * @throws IOException
     */
    public void finish() throws IOException {
        bw.write("]");
        bw.close();
    }
}