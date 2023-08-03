package com.atguigu.gmall.realtime.util;

import com.atguigu.gmall.realtime.common.GmallConstant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;

import javax.swing.plaf.metal.MetalIconFactory;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @title: JdbcUtil
 * @Author joey
 * @Date: 2023/7/31 20:30
 * @Version 1.0
 * @Note:
 */
public class JdbcUtil {


    public static Connection getMysqlConnection() throws ClassNotFoundException, SQLException {
        //加载mysql驱动
        Class.forName(GmallConstant.MYSQL_DRIVER);

        Class.forName(GmallConstant.MYSQL_DRIVER);
        //使用驱动管理器获取 mysql 连接
        String url = GmallConstant.MYSQL_URL;
        return DriverManager.getConnection(url, "root", "aaaaaa");

    }

    public static <T> List<T> queryList(Connection conn, String querySql,
                                        Object[] agrs,
                                        Class<T> tClass,
                                        boolean ... isUnderlineToCamel) {
        boolean defaultUnderlineToCamel = false;
        if (isUnderlineToCamel.length >0){
            defaultUnderlineToCamel = isUnderlineToCamel[0];
        }

        List<T> result = new ArrayList<>();
        //根据链接得到一个预处理语句
        try ( PreparedStatement ps = conn.prepareStatement(querySql)){

            //给sql中的占位符进行赋值
            for (int i = 0; agrs!= null &&i < agrs.length; i++) {
                Object agr = agrs[i];
                ps.setObject(i+1,agr);
            }
            //执行查询语句
            ResultSet resultSet = ps.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            //解析查询到结果，存储到list集合中
            //遍历所有行，每行封装到一个T类型的对象，存储到List集合中
            while (resultSet.next()) {
                T t = tClass.newInstance();
                // 通过反射  利用无惨构造器创建对象

                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    //获取列名, 对应 T 对象中的属性名
                    String name = metaData.getColumnLabel(i);
                    if (defaultUnderlineToCamel) {
                        name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                        // a_a => aA aa_aa_aa =>aa_AaAa
                    }
                    // 获取对应的列的值
                    Object value = resultSet.getObject(i);
                    //赋值到 T 对象中: t.name=value
                    BeanUtils.setProperty(t, name, value);

                }
                BeanUtils.setProperty(t, "op", "r");
                result.add(t);
            }

        }
         catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public static void closeConnection(Connection conn) throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

}
