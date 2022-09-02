package com.youfan.udf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IpBelongUDF extends UDF {

    public static List<String> map = new ArrayList();
    public static long[] start_from_index;
    public static long[] end_to_index;
    public static Map<Long, String> valueCache = new HashMap();



    private void LoadIPLocation() {
        Configuration conf = new Configuration();
        String uri = "hdfs://172.16.2.181:8020/data/tmp/ip.merge.txt";;
        FileSystem fs = null;
        FSDataInputStream in = null;
        BufferedReader d = null;
        try {
            fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path(uri));

            d = new BufferedReader(new InputStreamReader(in,"UTF-8"));
            String s = null;
            while (true) {
                s = d.readLine();
                if (s == null) {
                    break;
                }
                map.add(s);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }
    public static int binarySearch(long[] start, long[] end, long ip){
        int low = 0;
        int high = start.length - 1;
        while (low <= high) {
            int middle = (low + high) / 2;
            if ((ip >= start[middle]) && (ip <= end[middle]))
                return middle;
            if (ip < start[middle])
                high = middle - 1;
            else {
                low = middle + 1;
            }
        }
        return -1;
    }
    public static long ip2long(String ip)
    {
        if (ip.matches("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")) {
            String[] ips = ip.split("[.]");
            long ipNum = 0L;
            if (ips == null) {
                return 0L;
            }
            for (int i = 0; i < ips.length; i++) {
//                System.out.println(ips[i]);
                ipNum = ipNum << 8 | Long.parseLong(ips[i]);
            }

            return ipNum;
        }
        return 0L;
    }

    public String evaluate(String ip) {

        if (ip == null){
            return "请传入一个ip";
        }
        long ipLong = ip2long(ip);
//        String whichString = which.toString();

//        if ((!whichString.equals("IDC")) && (!whichString.equals("REGION")) && (!whichString.equals("CITY")))
//        {
//            return "Unknown Args!use(IDC or REGION or CITY)";
//        }

        if (map.size() == 0) {
            LoadIPLocation();
            start_from_index = new long[map.size()];
            end_to_index = new long[map.size()];
            for (int i = 0; i < map.size(); i++) {
//                StringTokenizer token = new StringTokenizer((String)map.get(i), "|");
//                token.nextToken();


                String startRe = map.get(i).split("[|]")[0];
                String endRe = map.get(i).split("[|]")[1];

                start_from_index[i] = ip2long(startRe);
                end_to_index[i] = ip2long(endRe);

//                start_from_index[i] = Long.parseLong(token.nextToken());
//                end_to_index[i] = Long.parseLong(token.nextToken());
            }

        }

        int ipindex = 0;
        if (!valueCache.containsKey(Long.valueOf(ipLong)))
        {
            ipindex = binarySearch(start_from_index, end_to_index, ipLong);
        }else {
            return valueCache.get(Long.valueOf(ipLong));
        }

        if (ipindex == -1) {
            return "Other IDC";
        }

        String[] location = ((String)map.get(ipindex)).split("[|]");
        String result = location[2] + "," +location[4] + ","+location[5]+","+location[6];

        valueCache.put(Long.valueOf(ipLong), result);
        return result;

    }

//    public static void main(String[] args) {
//        IpBelongUDF ipBelongUDF = new IpBelongUDF();
//        String evaluate = ipBelongUDF.evaluate("1.0.1.0");
//        System.out.println("evaluate = " + evaluate);
//    }

}
