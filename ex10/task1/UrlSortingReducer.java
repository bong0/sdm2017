import frm.*;
import java.util.*;


public class UrlSortingReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    /*
        For one user, determines the most visited urls (sort)
     */
    public static String DELIMITER = ";";
    @SuppressWarnings("unchecked")
    public void reduce(KEYIN key, Iterable<VALUEIN> value, Context<KEYOUT, VALUEOUT> context) {

        Comparator countUrlComparator = new Comparator<VALUEIN>() {
            public int compare(VALUEIN countUrl1, VALUEIN countUrl2) {
                Integer count1 = Integer.parseInt(((String)countUrl1).split(DetermineUserReducer.MAGICQOUTE)[0]);
                Integer count2 = Integer.parseInt(((String)countUrl2).split(DetermineUserReducer.MAGICQOUTE)[0]);

                //System.out.println("comparing "+count1+ " with "+count2);
                return count2.compareTo(count1);
            }
        };
        List<VALUEIN> urlCountSortedList = new ArrayList<>();

        for(VALUEIN countUrl : value){
            urlCountSortedList.add(countUrl);
        }
        Collections.sort(urlCountSortedList, countUrlComparator);


        // accumulate values to context without the counts in them
        StringBuilder rankedUrlList = new StringBuilder();
        for(VALUEIN countUrl : urlCountSortedList) {
            VALUEIN rankedUrl = (VALUEIN)((String)countUrl).split(DetermineUserReducer.MAGICQOUTE)[1];
            rankedUrlList.append(rankedUrl);
            rankedUrlList.append(DELIMITER);
        }
        context.write((KEYOUT)key, (VALUEOUT) rankedUrlList.toString());
    }
}

