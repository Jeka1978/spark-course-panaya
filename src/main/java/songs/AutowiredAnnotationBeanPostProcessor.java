package songs;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;

/**
 * Created by Evegeny on 14/07/2016.
 */
@Component
public class AutowiredAnnotationBeanPostProcessor implements BeanPostProcessor {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private ApplicationContext context;
    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        Class<?> beanClass = bean.getClass();
        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(AutowiredBroadcast.class)) {
                AutowiredBroadcast annotation = field.getAnnotation(AutowiredBroadcast.class);
                Class classOfBeanToInject = annotation.value();
                Object bean2Inject = context.getBean(classOfBeanToInject);
                Broadcast<Object> broadcast = sc.broadcast(bean2Inject);
                field.setAccessible(true);
                ReflectionUtils.setField(field,bean,broadcast);
            }
        }
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }
}
