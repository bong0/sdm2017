import frm.Context;
import frm.Reducer;

import java.util.HashMap;
import java.util.Map;


public class DetermineUserReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    public static String MAGICQOUTE = ";";

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(KEYIN key, Iterable<VALUEIN> value, Context<KEYOUT, VALUEOUT> context) {

        VALUEIN username = null; // username not yet known

        Map<VALUEIN, Integer> urlCountMap = new HashMap<>();
        for(VALUEIN singleValue : value){
            String singleValueAsString = (String)singleValue;
            if(singleValueAsString.startsWith(UserReader.MAGICQOUTE) &&
                    singleValueAsString.endsWith(UserReader.MAGICQOUTE)){
                // it's a username
                        username = (VALUEIN)singleValueAsString.replaceAll(UserReader.MAGICQOUTE, "");
            } else {
                // it's a URL
                urlCountMap.put(singleValue, urlCountMap.getOrDefault(singleValue, 0) + 1);
            }

        }
        // counts are computed, now output those with the url and username as key;

        //System.out.println(urlCountMap.toString());


        for(VALUEIN url: urlCountMap.keySet()) {
            //if(username.equals("np93kxke")) {
             //   System.out.println("k=" + username + "  v=" + urlCountMap.get(url) + MAGICQOUTE + url);
            //}
            context.write((KEYOUT) username, (VALUEOUT) (urlCountMap.get(url)+MAGICQOUTE+url));
        }

    }


}
