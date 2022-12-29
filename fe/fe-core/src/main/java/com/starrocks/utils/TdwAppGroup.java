// This file is made available under Elastic License 2.0.
package com.starrocks.utils;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwAppGroup {
    private String appGroupName = null;

    private String createTime = null;

    public TdwAppGroup() {

    }

    public String getAppGroupName() {
        return appGroupName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
