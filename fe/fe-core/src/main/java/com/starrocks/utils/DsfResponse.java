// This file is made available under Elastic License 2.0.
package com.starrocks.utils;
/**
 * Created by andrewcheng on 2022/9/16.
 */
public class DsfResponse {
    public int retCode;
    public String retMsg;
    public TdwResponse retObj;

    public int getRetCode() {
        return retCode;
    }

    public String getRetMsg() {
        return retMsg;
    }

    public TdwResponse getRetObj() {
        return retObj;
    }
}
