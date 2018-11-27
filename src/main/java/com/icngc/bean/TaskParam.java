package com.icngc.bean;

import lombok.Data;

import java.util.List;

/**
 * @author: Mr.Wang
 * @create: 2018-11-27 20:18
 */
@Data
public class TaskParam {
    /**
     * 年龄
     */
    private List<Integer> ages;

    /**
     * 性别
     */
    private List<String> genders;

    /**
     * 职业
     */
    private List<String> professionals;

    /**
     * 城市
     */
    private List<String> cities;
}
