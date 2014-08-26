/**
 * Created by wangwei on 14-8-19.
 */
public class TestSplit {
    private final static String text="13|15|1|0||640/360|||15000000|354832052581186||||{\"imei\":\"354832052581186\"}|1||||1||||3||110.159.236.73||||||||||||||||||1406898447|589|android-async-http/1.4.4 (http://loopj.com/android-async-http)||||||15|0|20000000|263|832|3465|640/360|1|3|7|1406882596|||1|13|15|0|15000|125.68.170.11|1406882598|||||||||||||";
    private final static String commatext="a,sdf,sdf,,grew,qwe,p12";
    public static void main(String[] args){
        //String [] data = text.split("\\|",-1);
        String [] data = commatext.split(",");
        for(int i=0;i<data.length;++i){
            System.out.println(data[i]);
        }
        System.out.println("LEN: "+data.length);
    }
}
