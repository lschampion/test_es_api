package com.lisacumt.test_es.API_test;

import lombok.Data;

import java.util.Date;

@Data
public class User {

    /**
     * erpId
     */
    private Long erpId;
    /**
     * 用户名
     */
    private String erpName;

    /**
     * 真实姓名
     */
    private String realName;

    /**
     * 性别。0表示男，1表示女，2代表未知
     */
    private Integer gender;

    /**
     * 头像
     */
    private String avatar;

    /**
     * 电话
     */
    private String phone;

    /**
     * 邮箱
     */
    private String email;
    /**
     * 密码
     */
    private String password;

    /**
     * 是否删除 0表示未删除，1表示已删除
     */
    private Integer isDeleted;

    /**
     * 0代表超管,1代表普通用户
     */
    private Integer priorityLevel;
    /**
     * 创建时间
     */
    private Date createTime;
    /**
     * 更新时间
     */
    private Date updateTime;
}