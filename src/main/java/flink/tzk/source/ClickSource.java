package flink.tzk.source;

import flink.tzk.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        //定义随机生成数据
        Random random = new Random();
        //定义字段选取数据集
        String[] users = {"Mary","Alice","tzk","kkk"};
        String[] urls = {"./home","./cart","./fav","./page=23?id=111"};
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user,url,timestamp));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}
