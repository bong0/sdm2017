import frm.*;
import java.util.*;


public class UrlSortingReducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    /*
        For one user, determines the most visited urls (sort)
     */
    public static String DELIMITER = ";";
    @SuppressWarnings("unchecked")
    public void reduce(KEYIN key, Iterable<VALUEIN> value, Context<KEYOUT, VALUEOUT> context) {

        class UrlWithCount {
            public String url;
            public Integer count;
            public UrlWithCount(String url, Integer count){
                this.url = url;
                this.count = count;
            }
        };

        Comparator countUrlComparator = new Comparator<UrlWithCount>() {
            public int compare(UrlWithCount countUrl1, UrlWithCount countUrl2) {

                return countUrl2.count.compareTo(countUrl1.count);
            }
        };

        Map<VALUEIN, Integer> urlCountMap = new HashMap<>();

        // first, aggregate the counts per url for all the devices
        for(VALUEIN countUrl : value){
            String url = ((String)countUrl).split(DetermineUserReducer.MAGICQOUTE)[1];
            Integer count = Integer.parseInt(((String)countUrl).split(DetermineUserReducer.MAGICQOUTE)[0]);
            urlCountMap.put((VALUEIN)url, urlCountMap.getOrDefault(url, 0) + count);
        }

        // now export the map to a list

        List<UrlWithCount> urlCountSortedList = new ArrayList<>();
        for(VALUEIN urlkey : urlCountMap.keySet()){
           // if(((String)key).equals("xh22hzgz")){
            //if(urlCountMap.get(urlkey) >= 2) {
            //    System.out.println(key+" "+(String) urlkey + " " + urlCountMap.get(urlkey));
            //}
            //}
            urlCountSortedList.add(new UrlWithCount((String)urlkey, urlCountMap.get(urlkey)));
        }
        // sort the list of urls by their count descending
        Collections.sort(urlCountSortedList, countUrlComparator);


        // accumulate values to context without the counts in them
        StringBuilder rankedUrlList = new StringBuilder();
        for(UrlWithCount rankedCountUrl : urlCountSortedList) {
            rankedUrlList.append(rankedCountUrl.url);
            rankedUrlList.append(DELIMITER);
        }
        context.write((KEYOUT)key, (VALUEOUT) rankedUrlList.toString());
    }
}

