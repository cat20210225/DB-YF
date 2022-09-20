package com.youfan.flume;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyInterceptor implements Interceptor {

    private ArrayList<Event> events = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);
        long ts = System.currentTimeMillis();

        String ModifyTime = jsonObject.getString("ModifyTime");
//        long ts = Long.parseLong(ModifyTime)/1000;
        if (ModifyTime != null){
            headers.put("timestamp",ModifyTime);
        }else {
            headers.put("timestamp",ts+"");
        }

        String table = jsonObject.getString("table");
        headers.put("table",table);
        jsonObject.remove("table");
        event.setBody(jsonObject.toString().getBytes());

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        events.clear();
        for (Event event : list) {
            events.add(intercept(event));
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}
