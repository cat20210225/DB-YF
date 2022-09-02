package com.youfan.udf;


import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.*;

public class ParentIdUDF extends UDF {

    public String evaluate(String id) throws ClassNotFoundException, SQLException {

        if(id==null){
            return null;
        }

        String flag = id;
        String jdbc = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://121.41.82.106:10000/db_ods";
        String user = "root";
        String password = "cgu1fem_DMD8faj_yae";
        Class.forName(jdbc);
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement statement = conn.createStatement();
        String querySQL = "select parentid from db_ods.ods_fx_basicdata where id ='"+flag+"' and CfgGroup = '产品类目'";
        ResultSet result = statement.executeQuery(querySQL);
        String parentId = null;
        if(result.next()){
            parentId = result.getString(1);
        }
        String resultParent = id;

        try {
            while (true){
                if (StringUtils.isNotBlank(parentId)){
                    resultParent = resultParent+","+parentId;
                    Statement statement2 = conn.createStatement();
                    querySQL = "select parentid from db_ods.ods_fx_basicdata where id ='"+parentId+"'";
                    ResultSet result2 = statement2.executeQuery(querySQL);
                    if(result2.next()){
                        parentId = result2.getString(1);
                    }else{
                        parentId = "";
                    }
                } else {
                    break;
                }
            }
            return resultParent;
        } finally {

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }



}
