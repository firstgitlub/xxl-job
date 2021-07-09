package com.xxl.job.core.biz.model;

import java.io.Serializable;

/**
 * Created by xuxueli on 2017-05-10 20:22:42
 *
 * RegistryParam registryParam =
 * new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appName, address);
 */
public class RegistryParam implements Serializable {
    private static final long serialVersionUID = 42L;

    //RegistryConfig.RegistType.EXECUTOR.name()
    private String registGroup;
    //appName
    private String registryKey;
    //address 应该是 执行器的地址
    //executorAddress  对，就是执行器的地址
    private String registryValue;

    public RegistryParam(){}
    public RegistryParam(String registGroup, String registryKey, String registryValue) {
        this.registGroup = registGroup;
        this.registryKey = registryKey;
        this.registryValue = registryValue;
    }

    public String getRegistGroup() {
        return registGroup;
    }

    public void setRegistGroup(String registGroup) {
        this.registGroup = registGroup;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public String getRegistryValue() {
        return registryValue;
    }

    public void setRegistryValue(String registryValue) {
        this.registryValue = registryValue;
    }

    @Override
    public String toString() {
        return "RegistryParam{" +
                "registGroup='" + registGroup + '\'' +
                ", registryKey='" + registryKey + '\'' +
                ", registryValue='" + registryValue + '\'' +
                '}';
    }
}
