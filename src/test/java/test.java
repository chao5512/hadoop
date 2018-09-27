/**
 * Created by wangchao on 2018/9/27.
 */
public class test {
    public static void main(String[] args) {
        String s = "1   w 23";
        String[] split = s.split("   ");
        for (String s1 : split) {
            System.out.println(s1);
        }
    }
}
