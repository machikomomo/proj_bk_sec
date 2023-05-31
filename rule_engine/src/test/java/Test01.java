import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

public class Test01 {
    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0;i<6;i++){
            sb.append(RandomUtils.nextInt(0, 10));
        }
        System.out.println(sb.toString());
//        int num = RandomUtils.nextInt(0, 10);
//        System.out.println(num);

        String s = StringUtils.leftPad(RandomUtils.nextInt(0, 10)+"", 5, "0");
        String account = StringUtils.leftPad(RandomUtils.nextInt(1, 10000) + "", 6, "0");
        System.out.println(s);
    }
}
