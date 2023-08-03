import com.alibaba.fastjson.JSONObject;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @title: demo01
 * @Author joey
 * @Date: 2023/7/31 16:29
 * @Version 1.0
 * @Note:
 */
public class demo01 {
    public static void main(String[] args) {

        JSONObject obj = new JSONObject();
        obj.put("a",97);
        obj.put("c",97);
        obj.put("d",97);

        List<String> columns = Arrays.asList("d,a".split(","));
        Set<String> keys = obj.keySet();


        // for (String key : keys) {
        //     if (!columns.contains(key)) {
        //         obj.remove(key);
        //     }
        // }

        // while (it.hasNext()){
        //     String key = it.next();
        //     if (!columns.contains(key)){
        //         it.remove();
        //     }
        // }

        keys.removeIf(key -> !columns.contains(key));




        System.out.println(keys);

    }
}
