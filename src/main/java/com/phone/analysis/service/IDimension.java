package com.phone.analysis.service;

import com.phone.analysis.mr.model.base.BaseDimension;

/**
 * @Description ：获取维度ID的接口
 * @Date 2019/1/5 10：28
 */
public interface IDimension {
    /**
     * 抽象方法，通过维度对象属性获取维度ID
     * @param baseDimension
     * @return
     */
    int getIDimensionByObject(BaseDimension baseDimension);
}