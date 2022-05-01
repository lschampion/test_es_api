## 测试ES7的 JAVA api

参考文章：

简单使用docker搭建ES环境

https://blog.csdn.net/chenweifu365/article/details/124097897

kerberos认证下各种方式连接elasticsearch研究与方案

https://www.cnblogs.com/zhouwenyang/p/14477427.html



JAVA api 参考：

https://www.cnblogs.com/shine-rainbow/p/15500048.html

https://blog.csdn.net/Imflash/article/details/101147730

https://blog.csdn.net/weixin_45321681/article/details/115585128



推荐使用 elasticuve 和 elasticsearch-head 两个浏览器插件 

报错：Fielddata is disabled on text fields by default. Set fielddata=true

参考：https://blog.csdn.net/caizhengwu/article/details/79743499

```json
PUT  /user/_mapping
{
  "properties": {
    "age": {
      "type": "text",
      "fielddata": true
    }
  }
}
```



