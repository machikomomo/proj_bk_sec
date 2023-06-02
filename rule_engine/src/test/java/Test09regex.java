/**
 * ^ start
 * $ end
 * * 0-max {0,}
 * + 1-max {1,}
 * ? 0次或1次 {0,1}
 * . 任何单个字符 除了/r/n（回车）
 * .* 经常一起用 任意个字符出现任意多次
 * x[abc]y abc中出现随便一个(只能一个）xay 可以匹配
 * x[abc]*y 可以匹配 xaby
 */

public class Test09regex {
    public static void main(String[] args) {
        String s1 = "1.*3.*5";
        String s2 = "12.*3";
        String s3 = "1(\\d{5})2";
        String s4 = "1(\\d+)2";
        String s5 = "12{3}.*3";
        String s6 = "12{2,3}3";
        String s7 = "1(?!2).*3"; // 零宽断言 【？！不能有】 【？=必须有】// 
        String s8 = "1(?![24].*3";
        String s9 = "1(?=[3,5]).*2";
    }
}
