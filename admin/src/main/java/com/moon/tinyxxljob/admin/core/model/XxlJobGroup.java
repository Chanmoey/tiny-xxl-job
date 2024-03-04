package com.moon.tinyxxljob.admin.core.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author Chanmoey
 * Create at 2024/2/27
 */
public class XxlJobGroup {

    private int id;
    //执行器中配置的项目名称
    private String appname;
    //中文名称
    private String title;
    //执行器的注册方法，0为自动注册，1为手动注册
    //这里其实很容易理解，web界面是可以手动录入执行器地址的，同时启动执行器
    //执行器也会自动将自己注册到调度中心的服务器中
    private int addressType;
    //执行器的地址，地址为IP+Port，不同的地址用逗号分开
    private String addressList;
    //更新时间
    private Date updateTime;
    //这里实际上就是把addressList属性中的多个地址转变成list集合了，
    //集合中存储的就是注册的所有执行器地址
    private List<String> registryList;

    /**
     * 将地址字符串变成集合
     */
    public List<String> getRegistryList() {
        if (addressList != null && !addressList.trim().isEmpty()) {
            registryList = new ArrayList<>(Arrays.asList(addressList.split(",")));
        }
        return registryList;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getAppname() {
        return appname;
    }

    public void setAppname(String appname) {
        this.appname = appname;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
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

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public void setAddressList(String addressList) {
        this.addressList = addressList;
    }

}
