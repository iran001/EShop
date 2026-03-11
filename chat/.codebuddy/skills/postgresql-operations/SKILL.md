---
# YAML 元数据
name: postgresql-operations
description: 仅用于准确查询门店销售数据、会员信息、商品信息、历史天气情况
version: 1.0.0
author: Great
tags: [postgresql, sql, query, select, database]
triggers:
  - pattern: "数据查询"
  - pattern: "销售数据"
  - pattern: "会员"
  - pattern: "商品"
  - pattern: "天气"
  - pattern: "营业额"
allowed-tools: 
disable: false
---

# PostgreSQL 查询技能手册
## 核心能力
仅处理 PostgreSQL 的 SELECT 类查询场景，不涉及数据修改、备份、优化等操作，覆盖以下常用场景查询：
- 营业额查询
- 会员信息查询
- 商品信息查询
- 历史天气查询

## 基础表结构说明
以下是核心业务表的结构定义（生成 SQL 时请基于这些表结构）：

### 1. 销售记录表（sell_records）
| 字段名                | 数据类型         | 说明                    | 示例值                      |
|-----------------------|-----------------|-------------------------|-----------------------------|
| id                    | int             | 记录唯一ID（主键）       | 1001                        |
| dh_id                 | varchar(50)     | 单号                    |"3100010001230101000032"     |
| vip_id                | varchar(50)     | 会员ID                  | "00010026"                  |
| vip_name              | varchar(100)    | 会员名称                | "张三"                      |
| yw_time               | timestamp       | 营收时间                | "2026-01-10 14:30:00"       |
| pro_old_total_money   | numeric(10,2)   | 原价总金额（元）         | 1999.99                    |
| yh_money              | numeric(10,2)   | 优惠金额（元）           | 10.0                       |
| yf_money              | numeric(10,2)   | 营业额（元）             | 200.0                      |
| pay_way               | varchar(50)     | 支付方式                 | "微信"                     |   

### 2. 销售明细表（sell_details）
| 字段名                | 数据类型         | 说明                    | 示例值                      |
|-----------------------|-----------------|-------------------------|-----------------------------|
| id                    | int             | 记录唯一ID（主键）       | 1001                        |
| dh_id                 | varchar(50)     | 单号                    |"3100010001230101000032"     |
| pro_id                | varchar(50)     | 商品ID                  | "00010026"                  |
| pro_name              | varchar(100)    | 商品名称                | "苹果"                      |
| pro_unit              | varchar(20)     | 单位                    | "公斤"                      |
| pro_num               | numeric(10,3)   | 数量                     | 1.000                      |
| pro_price             | numeric(10,2)   | 单价（元）               | 200.0                      |
| pro_total_price       | numeric(10,2)   | 金额（元）               | 20.0                       |
| yw_time               | timestamp       | 营收时间                | "2026-01-10 14:30:00"       |
| cls_name              | varchar(50)     | 目录类型                | "水果"                      |

### 3. 会员信息表（vip_info）
| 字段名                | 数据类型         | 说明                    | 示例值                      |
|-----------------------|-----------------|-------------------------|-----------------------------|
| id                    | int             | 记录唯一ID（主键）       | 1001                        |
| vip_id                | varchar(50)     | 会员ID                  | "00010026"                  |
| vip_name              | varchar(100)    | 会员名称                | "张三"                      |
| vip_phone             | varchar(20)     | 手机号                  | "15067876543"               |
| vip_xf_total_money    | numeric(10,2)   | 累计金额（元）           | 2000.0                     |
| vip_last_xf_time      | timestamp       | 上次消费时间             | "2026-01-10 14:30:00"       |

### 4. 商品信息表（pro_info）
| 字段名                | 数据类型         | 说明                    | 示例值                      |  
|-----------------------|-----------------|-------------------------|-----------------------------|
| id                    | int             | 记录唯一ID（主键）       | 1001                        |
| pro_id                | varchar(50)     | 商品ID                  | "00010026"                  |
| pro_name              | varchar(100)    | 商品名称                | "苹果"                      |
| unit                  | varchar(20)     | 单位                    | "公斤"                      |
| sale_price            | numeric(10,2)   | 售价（元）              | 200.0                       |
| price_way             | varchar(50)     | 计价方式                | "计重"                      |
| cls_name              | varchar(50)     | 目录类型                | "水果"                      |

### 5. 历史天气表（weather_history）
| 字段名                | 数据类型         | 说明                    | 示例值                       |
|-----------------------|-----------------|-------------------------|------------------------------|
| id                    | int             | 记录唯一ID（主键）       | 1001                         |
| weather_date          | date            | 日期                     | '2022-11-03'                |
| temp_max              | numeric(5,2)    | 最高温度                 | 20.50                       |
| temp_min              | numeric(5,2)    | 最低温度                 | 10.50                       |
| precipitation         | numeric(5,2)    | 降水量                   | 12                          |
| weather_desc          | varchar(50)     | 天气描述                 | "中雨"                      |

## 场景查询
### 1. 销售数据查询
```sql
-- 查询指定日期销售单
SELECT id,dh_id,vip_id,vip_name,yw_time, pro_old_total_money,yh_money,yf_money,pay_way
FROM  public.sell_records WHERE yw_time BETWEEN '2025-12-23' AND '2025-12-23 23:59:59'

-- 查询日期范围内销售总额
SELECT SUM(yf_money) AS yf_money
FROM  public.sell_records WHERE yw_time BETWEEN '2025-12-20' AND '2025-12-23 23:59:59'

-- 查询日期范围内销售商品排行
SELECT pro_name,SUM(pro_num) AS pro_num
FROM  sell_records a
INNER JOIN sell_details b on a.dh_id = b.dh_id
WHERE a.yw_time BETWEEN '2025-12-20' AND '2025-12-23 23:59:59'
GROUP BY pro_name
ORDER BY SUM(pro_num) DESC

-- 查询指定会员在一定时间范围内消费金额
SELECT SUM(yf_money) AS yf_money
FROM  sell_records a
WHERE a.yw_time BETWEEN '2025-12-20' AND '2025-12-23 23:59:59'
AND a.vip_name = '陈雅君'

```

### 2. 会员数据查询
```sql
-- 指定会员信息查询
SELECT id,vip_id,vip_name,vip_phone,vip_xf_total_money,vip_last_xf_time 
FROM vip_info  WHERE vip_name = '陈雅君'

```

### 3.商品信息查询
```sql
-- 查询所有商品信息
SELECT id,pro_id,pro_name,unit,sale_price,price_way,cls_name
FROM pro_info

```

### 4.天气信息查询
```sql
-- 查询指定日期天气情况
SELECT id,weather_date,temp_max,temp_min, precipitation,weather_desc
FROM weather_history
WHERE weather_date = '2022-11-04'

```

## 使用规范
1.所有查询仅使用 SELECT 语句，不包含 INSERT/UPDATE/DELETE 等修改操作，避免数据风险；
2.复杂查询建议先通过 LIMIT 10 测试结果正确性，再移除分页条件；
