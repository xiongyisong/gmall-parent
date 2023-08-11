package com.atguigu.gmall.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @title: IkUtil
 * @Author joey
 * @Date: 2023/8/11 17:50
 * @Version 1.0
 * @Note:
 */
public class IkUtil {
    public static Set<String> split(String keyword) {
        HashSet<String> set = new HashSet<>();
        // String => Reader
        // 内存流: StringReader
        // 参数 1: 字符输入流  参数 2:是否启用只能分词  smart  max_word
        IKSegmenter seg = new IKSegmenter(new StringReader(keyword), true);
        try {
            Lexeme next = seg.next();
            while (next != null) {
                String kw = next.getLexemeText();
                set.add(kw);
                next = seg.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return set;
    }
    public static void main(String[] args) {
        Set<String> set = split("我是中国人");
        for (String s : set) {
            System.out.println("s = " + s);
        }
    }

}
