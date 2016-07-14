package songs;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

/**
 * Created by Evegeny on 14/07/2016.
 */
public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringConfig.class);
        MusicJudge musicJudge = context.getBean(MusicJudge.class);
        List<String> list = musicJudge.topX("data/songs/beatles/*", 3);
        System.out.println(list);
    }
}
