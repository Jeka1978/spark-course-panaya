package songs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Evegeny on 14/07/2016.
 */
@Component
public class UserProperties implements Serializable {
    public List<String> garbage;


    @Value("${garbage}")
    private void initGarbage(String[] garbage) {
        this.garbage = Arrays.asList(garbage);
    }


}
