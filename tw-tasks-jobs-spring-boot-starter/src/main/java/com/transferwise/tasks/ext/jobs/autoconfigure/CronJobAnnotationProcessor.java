package com.transferwise.tasks.ext.jobs.autoconfigure;

import static com.transferwise.tasks.impl.jobs.interfaces.IJob.ProcessResult.ResultCode.SUCCESS;

import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.impl.jobs.CronJob;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.scheduling.support.SimpleTriggerContext;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.util.StringValueResolver;

@Slf4j
@Service
public class CronJobAnnotationProcessor implements BeanFactoryPostProcessor, BeanPostProcessor, EmbeddedValueResolverAware {

  private StringValueResolver embeddedValueResolver;

  private final Set<Class<?>> nonAnnotatedClasses = Collections.newSetFromMap(new ConcurrentHashMap<>(64));
  private ConfigurableListableBeanFactory beanFactory;

  @Override
  public void setEmbeddedValueResolver(StringValueResolver resolver) {
    this.embeddedValueResolver = resolver;
  }

  @Override
  public Object postProcessBeforeInitialization(Object bean, String beanName) {
    if (bean instanceof AopInfrastructureBean || bean instanceof TaskScheduler || bean instanceof ScheduledExecutorService) {
      // Ignore AOP infrastructure such as scoped proxies.
      return bean;
    }

    Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
    if (!this.nonAnnotatedClasses.contains(targetClass) && AnnotationUtils.isCandidateClass(targetClass, CronJob.class)) {
      Map<Method, List<CronJob>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
          (MethodIntrospector.MetadataLookup<List<CronJob>>) method -> {
            Set<CronJob> cronJobAnnotations = AnnotatedElementUtils.getAllMergedAnnotations(method, CronJob.class);
            return (!cronJobAnnotations.isEmpty() ? cronJobAnnotations.stream().collect(Collectors.toList()) : null);
          });
      if (annotatedMethods.isEmpty()) {
        this.nonAnnotatedClasses.add(targetClass);
        log.trace("No @CronJob annotations found on bean class: {}", targetClass);
      } else {
        // Non-empty set of methods
        annotatedMethods.forEach((method, annotations) -> {
          for (int i = 0; i < annotations.size(); i++) {
            addJob(annotations.get(i), method, bean, beanName, i);
          }
        });
        if (log.isTraceEnabled()) {
          log.trace("{} @CronJob methods processed on bean '{}': {}", annotatedMethods.size(), beanName, annotatedMethods);
        }
      }
    }
    return bean;
  }

  private void addJob(CronJob cronJob, Method method, Object bean, String beanName, int num) {
    String uniqueName = buildUniqueName(method, beanName, num);
    CronTrigger trigger = toCronTrigger(cronJob, method);
    Runnable runnable = createRunnable(bean, method);
    beanFactory.registerSingleton(uniqueName, new CronJobHandler(uniqueName, trigger, runnable, cronJob.transactional()));
  }

  private static String buildUniqueName(Method method, String beanName, int num) {
    return "(" + method.getDeclaringClass().getCanonicalName() + ")" + beanName + ":_#" + num + "_" + method;
  }

  private CronTrigger toCronTrigger(CronJob cronJob, Method method) {
    String cron = cronJob.value();
    String zone = cronJob.timezone();

    if (this.embeddedValueResolver != null) {
      cron = this.embeddedValueResolver.resolveStringValue(cron);
      zone = this.embeddedValueResolver.resolveStringValue(zone);
    }

    if (!StringUtils.hasText(cron)) {
      throw new IllegalArgumentException(
          "Cron expression is mandatory field for 'CronJob' annotation. Check method '" + method + "' of class " + method.getDeclaringClass()
              .getCanonicalName());
    }

    TimeZone timeZone = StringUtils.hasText(zone) ? TimeZone.getTimeZone(zone) : TimeZone.getDefault();

    return new CronTrigger(cron, timeZone);
  }


  private static Runnable createRunnable(Object target, Method method) {
    Assert.isTrue(method.getParameterCount() == 0, "Only no-arg methods may be annotated with @CronJob");
    Method invocableMethod = AopUtils.selectInvocableMethod(method, target.getClass());
    return new ScheduledMethodRunnable(target, invocableMethod);
  }


  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
    this.beanFactory = beanFactory;
  }

  @AllArgsConstructor
  private static class CronJobHandler implements IJob {

    private final String uniqueName;
    private final CronTrigger trigger;
    private final Runnable handler;
    private final boolean transactional;

    @Override
    public String getUniqueName() {
      return uniqueName;
    }

    @Override
    public boolean isTransactional() {
      return transactional;
    }

    @Override
    public ZonedDateTime getNextRunTime() {
      Date date = trigger.nextExecutionTime(new SimpleTriggerContext());
      return date == null ? null : date.toInstant().atZone(ZoneId.systemDefault());
    }

    @Override
    public ProcessResult process(ITask task) {
      handler.run();
      return new ProcessResult().setResultCode(SUCCESS);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CronJobHandler that = (CronJobHandler) o;
      return Objects.equals(uniqueName, that.uniqueName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(uniqueName);
    }
  }
}