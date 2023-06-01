import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test05Pattern {
    public static void main(String[] args) {
        String seq = "ABAAACBBBCF";
//        String p = "A.*F.*C";
//        String p = "A.*C.*F";
//        String p = "AB.*C";
        String p = "AA";
        // api
        Pattern r = Pattern.compile(p);
        Matcher matcher = r.matcher(seq);
        int count = 0;
        while (matcher.find()){
            count++;
        }
        System.out.println(count);
    }
}
