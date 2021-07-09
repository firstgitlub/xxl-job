package com.xxl.job.executor.core.config;

import com.xxl.job.core.executor.XxlJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * xxl-job config
 * 执行器组件，配置文件地址：
 *
 * @author xuxueli 2017-04-28
 */
@Configuration
@ComponentScan(basePackages = "com.xxl.job.executor.service.jobhandler")
public class XxlJobConfig {
    private Logger logger = LoggerFactory.getLogger(XxlJobConfig.class);

    /**
     * ### 调度中心部署跟地址 [选填]：如调度中心集群部署存在多个地址则用逗号分隔。
     * # 执行器将会使用该地址进行"执行器心跳注册"和"任务结果回调"；为空则关闭自动注册；
     *
     * 主要用于执行器与调度中心通信使用
     */
    @Value("${xxl.job.admin.addresses}")
    private String addresses;

    //执行器集群部署时  分组调度使用
    @Value("${xxl.job.executor.appname}")
    private String appname;

    /**
     * ### 执行器IP [选填]：默认为空表示自动获取IP，多网卡时可手动设置指定IP，
     * # 该IP不会绑定Host仅作为通讯实用；地址信息用于 "执行器注册" 和 "调度中心请求并触发任务"；
     */
    @Value("${xxl.job.executor.ip}")
    private String ip;

    /**
     * ### 执行器端口号 [选填]：小于等于0则自动获取；默认端口为9999，
     * # 单机部署多个执行器时，注意要配置不同执行器端口；
     */
    @Value("${xxl.job.executor.port}")
    private int port;

    /**
     * ### 执行器运行日志文件存储磁盘路径 [选填] ：需要对该路径拥有读写权限；为空则使用默认路径；
     */
    @Value("${xxl.job.executor.logpath}")
    private String logpath;

    /**
     * 执行器通讯TOKEN [选填]：非空时启用；
     */
    @Value("${xxl.job.accessToken}")
    private String accessToken;

    /**
     *执行器组件，配置内容说明：
     */
    @Bean(initMethod = "start", destroyMethod = "destroy")
    public XxlJobExecutor xxlJobExecutor() {
        logger.error("------------ xxlJobExecutor -----------");
        XxlJobExecutor xxlJobExecutor = new XxlJobExecutor();
        xxlJobExecutor.setIp(ip);
        xxlJobExecutor.setPort(port);
        xxlJobExecutor.setAppName(appname);
        xxlJobExecutor.setAdminAddresses(addresses);
        xxlJobExecutor.setLogPath(logpath);
        xxlJobExecutor.setAccessToken(accessToken);
        return xxlJobExecutor;
    }

}