// This file is made available under Elastic License 2.0.
package com.starrocks.utils;

/**
 * Created by andrewcheng on 2022/9/16.
 */
public class TdwResponse {
    public int code;
    public String message;
    public String solution;

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public String getSolution() {
        return solution;
    }
}
