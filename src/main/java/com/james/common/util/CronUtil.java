package com.james.common.util;


public class CronUtil {
    // private static final Logger LOGGER =
    // LoggerFactory.getLogger(CronUtil.class);
    //
    // public static boolean isCronStringValid(String cron) {
    // boolean ret = false;
    // try {
    // CronExpression cExpression = new CronExpression(cron);
    // cExpression.getFinalFireTime();
    // ret = true;
    // } catch (ParseException e) {
    // ret = false;
    // }
    // return ret;
    // }
    //
    // /**
    // * 获取Cron表达式的执行周期 现在暂不支持范围性任务的周期解析:* 10,20,30 12 * * ?
    // *
    // * @param cronStr
    // * cron表达式
    // * @return 执行周期（毫秒）
    // * @throws ParseException
    // */
    // public static Long getCronExcuteCycle(String cronStr) {
    // Long excute_cycle = 1000000000000l;
    // try {
    // CronExpression cExpression = new CronExpression(cronStr);
    // Date now_time = new Date();
    // Date firstTime = cExpression.getNextValidTimeAfter(now_time);
    // Date nextTime = cExpression.getNextValidTimeAfter(firstTime);
    // excute_cycle = nextTime.getTime() - firstTime.getTime();
    // } catch (ParseException e) {
    // LOGGER.error("get cron execute cycle from cron string {} failed.",
    // cronStr);
    // e.printStackTrace();
    // }
    // return excute_cycle;
    // }
}
