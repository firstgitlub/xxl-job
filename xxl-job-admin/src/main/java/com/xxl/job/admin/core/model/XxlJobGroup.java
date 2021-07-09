package com.xxl.job.admin.core.model;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by xuxueli on 16/9/30.
 *
 * AppName: 是每个执行器集群的唯一标示AppName, 执行器会周期性以AppName为对象进行自动注册。可通过该配置自动发现注册成功的执行器, 供任务调度时使用;
 * 名称: 执行器的名称, 因为AppName限制字母数字等组成,可读性不强, 名称为了提高执行器的可读性;
 * 排序: 执行器的排序, 系统中需要执行器的地方,如任务新增, 将会按照该排序读取可用的执行器列表;
 * 注册方式：调度中心获取执行器地址的方式；
 *         自动注册：执行器自动进行执行器注册，调度中心通过底层注册表可以动态发现执行器机器地址；
 *         手动录入：人工手动录入执行器的地址信息，多地址逗号分隔，供调度中心使用；
 * 机器地址："注册方式"为"手动录入"时有效，支持人工维护执行器的地址信息；
 */
public class XxlJobGroup {

    private int id;
    private String appName; //这个是分组的？
    private String title;
    private int order;
    private int addressType;    // 执行器地址类型：0=自动注册、1=手动录入

    //这个值是在初始化的时候 设置的  在这个类JobRegistryMonitorHelper 里面做的
    private String addressList;    // 执行器地址列表，多地址逗号分隔(手动录入)

    // registry list
    private List<String> registryList;  // 执行器地址列表(系统注册)
    public List<String> getRegistryList() {
        if (StringUtils.isNotBlank(addressList)) {
            registryList = new ArrayList<String>(Arrays.asList(addressList.split(",")));
        }
        return registryList;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getAddressType() {
        return addressType;
    }

    public void setAddressType(int addressType) {
        this.addressType = addressType;
    }

    public String getAddressList() {
        return addressList;
    }

    public void setAddressList(String addressList) {
        this.addressList = addressList;
    }

}
