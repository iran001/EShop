"""
数据库查询 Skill
基于 postgresql-operations Skill 规范实现
支持PostgreSQL数据库操作 - 仅SELECT查询
使用 frontmatter 从 SKILL.md 加载配置和模板
"""
import json
import os
import re
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from config import DB_CONFIG, BASE_DIR
from .base_skill import BaseSkill

# 可选依赖
try:
    import frontmatter
    FRONTMATTER_AVAILABLE = True
except ImportError:
    FRONTMATTER_AVAILABLE = False

try:
    from langchain_core.tools import Tool
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


class DatabaseSkill(BaseSkill):
    """
    数据库查询技能
    基于 postgresql-operations Skill 规范
    仅支持 SELECT 查询，不涉及数据修改操作
    """
    
    name = "database_query"
    description = "用于查询PostgreSQL数据库的工具。基于postgresql-operations规范，仅支持SELECT查询。"
    SKILL_PATH = BASE_DIR / ".codebuddy" / "skills" / "postgresql-operations" / "SKILL.md"
    
    # SQL 安全验证配置
    FORBIDDEN_KEYWORDS = frozenset({
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'TRUNCATE',
        'CREATE', 'ALTER', 'GRANT', 'REVOKE', 'EXECUTE'
    })
    
    # 模板默认值
    TEMPLATE_DEFAULTS = {
        "fields": "*",
        "sort_type": "DESC",
        "sort_field": "id",
        "page_size": 10,
        "page": 1,
    }
    
    def __init__(self):
        self.db_config = DB_CONFIG
        self._schema_cache: Optional[List[Dict]] = None
        self._skill_data: Optional[Dict] = None
        self._query_templates: Optional[Dict[str, str]] = None
        # 从 SKILL.md 加载触发词和关键词
        self._triggers: List[str] = []
        self._query_keywords: List[str] = []
        self._load_triggers_and_keywords()
        self._load_skill()

    def _load_triggers_and_keywords(self) -> None:
        """从 SKILL.md 加载触发词和关键词"""
        skill_path = str(self.SKILL_PATH)
        
        # 默认配置
        default_triggers = [
            "查询", "聚合查询", "窗口函数",
            "订单", "营业额", "销售", "会员", "商品", "天气"
        ]
        default_keywords = [
            "查询", "查一下", "查数据", "查表", "查数据库",
            "select", "show", "list",
            "订单", "营业额", "销售", "会员", "商品", "天气"
        ]
        
        if not os.path.exists(skill_path):
            self._triggers = default_triggers
            self._query_keywords = default_keywords
            return
        
        try:
            if FRONTMATTER_AVAILABLE:
                skill_doc = frontmatter.load(skill_path)
                # 从 triggers 读取触发词
                triggers = skill_doc.get("triggers", [])
                file_triggers = [t.get("pattern") for t in triggers if t.get("pattern")]
                
                if file_triggers:
                    self._triggers = file_triggers
                    # 关键词包含触发词和一些通用查询词
                    self._query_keywords = file_triggers + [
                        "查询", "查一下", "查数据", "查表", "查数据库",
                        "select", "show", "list"
                    ]
                else:
                    self._triggers = default_triggers
                    self._query_keywords = default_keywords
            else:
                # 备用方案：使用默认配置
                self._triggers = default_triggers
                self._query_keywords = default_keywords
        except Exception as e:
            print(f"加载触发词失败: {e}，使用默认配置")
            self._triggers = default_triggers
            self._query_keywords = default_keywords

    def _load_skill(self) -> None:
        """加载 PostgreSQL Skill 配置"""
        skill_path = str(self.SKILL_PATH)
        
        if not os.path.exists(skill_path):
            self._skill_data = self._get_default_skill_data()
            self._query_templates = self._get_default_templates()
            return

        try:
            if FRONTMATTER_AVAILABLE:
                skill_doc = frontmatter.load(skill_path)
                self._skill_data = {
                    "name": skill_doc.get("name", "postgresql-operations"),
                    "triggers": [p.get("pattern") for p in skill_doc.get("triggers", [])],
                    "description": skill_doc.get("description", ""),
                    "content": skill_doc.content
                }
            else:
                with open(skill_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                # 使用已加载的触发词或默认触发词
                triggers = self._triggers if self._triggers else [
                    "查询", "PostgreSQL查询", "postgres select"
                ]
                self._skill_data = {
                    "name": "postgresql-operations",
                    "triggers": triggers[:3] if len(triggers) >= 3 else triggers,
                    "description": "PostgreSQL 纯查询技能",
                    "content": content
                }
            
            self._query_templates = self._parse_templates_from_skill()
        except Exception as e:
            print(f"加载 Skill 文件失败: {e}，使用默认配置")
            self._skill_data = self._get_default_skill_data()
            self._query_templates = self._get_default_templates()

    def _get_default_skill_data(self) -> Dict[str, Any]:
        """从 SKILL.md 获取 Skill 配置"""
        skill_path = str(self.SKILL_PATH)
        
        # 使用已加载的触发词或默认触发词
        triggers = self._triggers if self._triggers else [
            "PG查询", "PostgreSQL查询", "postgres select"
        ]
        
        if not os.path.exists(skill_path):
            # 文件不存在时返回硬编码默认值
            return {
                "name": "postgresql-operations",
                "triggers": triggers,
                "description": "仅用于PostgreSQL数据库查询的技能",
                "content": self._get_fallback_skill_content()
            }
        
        try:
            if FRONTMATTER_AVAILABLE:
                skill_doc = frontmatter.load(skill_path)
                file_triggers = [t.get("pattern") for t in skill_doc.get("triggers", []) if t.get("pattern")]
                return {
                    "name": skill_doc.get("name", "postgresql-operations"),
                    "triggers": file_triggers if file_triggers else triggers,
                    "description": skill_doc.get("description", ""),
                    "content": skill_doc.content
                }
            else:
                # 备用方案：直接读取文件内容
                with open(skill_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return {
                    "name": "postgresql-operations",
                    "triggers": triggers,
                    "description": "PostgreSQL 查询技能",
                    "content": content
                }
        except Exception as e:
            print(f"从 SKILL.md 加载配置失败: {e}")
            return {
                "name": "postgresql-operations",
                "triggers": triggers,
                "description": "PostgreSQL 查询技能",
                "content": self._get_fallback_skill_content()
            }

    def _get_fallback_skill_content(self) -> str:
        """获取 Skill Markdown 内容，优先从 SKILL.md 文件读取"""
        skill_path = str(self.SKILL_PATH)
        
        # 尝试从 SKILL.md 文件读取内容
        if os.path.exists(skill_path):
            try:
                if FRONTMATTER_AVAILABLE:
                    skill_doc = frontmatter.load(skill_path)
                    return skill_doc.content
                else:
                    with open(skill_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    return content
            except Exception as e:
                print(f"从 SKILL.md 读取内容失败: {e}")
        
        # 文件不存在或读取失败时返回完整的默认技能内容
        return """# PostgreSQL 查询技能手册
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
| yf_money              | numeric(10,2)   | 营业额（元）         | 200.0                      |
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
FROM vip_info  WHERE vip_name = '陈君'
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
"""

    def _get_default_templates(self) -> Dict[str, str]:
        """从 SKILL.md 获取查询模板，如果不存在则使用默认模板"""
        skill_path = str(self.SKILL_PATH)
        
        # 基础默认模板
        default_templates = {
            "simple_select": """SELECT {fields} FROM {table_name} 
WHERE {condition} ORDER BY {sort_field} {sort_type} 
LIMIT {page_size} OFFSET {offset};""",
            
            "like_search": """SELECT {fields} FROM {table_name} 
WHERE {field} ILIKE '%{keyword}%';""",
            
            "range_query": """SELECT {fields} FROM {table_name} 
WHERE {range_condition} ORDER BY {sort_field} {sort_type};""",
            
            "inner_join": """SELECT {fields} FROM {table1} a
INNER JOIN {table2} b ON a.{join_field1} = b.{join_field2}
WHERE {condition};""",
            
            "left_join": """SELECT {fields} FROM {table1} a
LEFT JOIN ({subquery}) b ON a.{join_field1} = b.{join_field2};""",
            
            "aggregation": """SELECT 
  COUNT(*) AS total_count,
  SUM({sum_field}) AS total_sum,
  AVG({avg_field}) AS avg_value,
  MAX({max_field}) AS max_value,
  MIN({min_field}) AS min_value
FROM {table_name} WHERE {condition};""",
            
            "group_by": """SELECT 
  DATE_TRUNC('{truncate_unit}', {time_field}) AS period,
  COUNT(*) AS count,
  SUM({sum_field}) AS total
FROM {table_name} WHERE {condition}
GROUP BY period ORDER BY period DESC;"""
        }
        
        if not os.path.exists(skill_path):
            return default_templates
        
        try:
            # 尝试从 SKILL.md 内容中提取 SQL 模板
            if FRONTMATTER_AVAILABLE:
                skill_doc = frontmatter.load(skill_path)
                content = skill_doc.content
            else:
                with open(skill_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            
            # 从 Markdown 代码块中提取 SQL 模板
            sql_blocks = re.findall(
                r'```sql\s*\n(.*?)\n```',
                content,
                re.DOTALL | re.IGNORECASE
            )
            
            if sql_blocks:
                # 如果提取到 SQL 块，创建基于场景的模板
                templates = default_templates.copy()
                
                # 添加从 SKILL.md 提取的特定场景模板
                for i, sql in enumerate(sql_blocks[:4], 1):  # 最多取前4个
                    templates[f"scene_query_{i}"] = sql.strip()
                
                return templates
            
            return default_templates
            
        except Exception as e:
            print(f"从 SKILL.md 加载模板失败: {e}")
            return default_templates

    def _parse_templates_from_skill(self) -> Dict[str, str]:
        """从 Skill Markdown 内容解析查询模板"""
        # 目前使用默认模板，未来可扩展为从 Markdown 智能解析
        return self._get_default_templates()

    def is_skill_triggered(self, user_input: str) -> bool:
        """判断用户输入是否触发 PostgreSQL 查询 Skill"""
        if not user_input:
            return False
            
        user_input_lower = user_input.lower()
        
        # 使用从 SKILL.md 加载的触发词（实例属性优先，其次是 skill_data）
        triggers = self._triggers if self._triggers else []
        if not triggers and self._skill_data:
            triggers = self._skill_data.get("triggers", [])
        
        # 检查配置的触发词
        if any(t and t.lower() in user_input_lower for t in triggers):
            return True
        
        # 检查通用查询关键词
        keywords = self._query_keywords if self._query_keywords else []
        return any(kw in user_input_lower for kw in keywords)

    def build_skill_prompt(self, user_input: str) -> str:
        """构建 Skill 专属 Prompt"""
        skill_content = self._skill_data.get("content", "")
        schema_summary = self.get_database_schema_summary()
        
        # 获取允许的表和字段列表
        allowed_tables = self._get_allowed_tables_and_fields()
        
        return f"""你是 PostgreSQL 查询专家，必须严格遵循以下技能规则生成 SQL：

{skill_content}

【极其重要 - 严格限制】
1. 你只能使用下面"当前数据库结构信息"中列出的表和字段
2. 禁止自行推断、猜测或使用任何未在 SKILL.md 中定义的表或字段
3. 如果用户问题涉及未定义的表或字段，请明确告知用户无法查询
4. 联表查询只能使用 SKILL.md 中定义的关联关系（如 sell_records.dh_id = sell_details.dh_id）

要求：
1. 仅生成 SELECT 语句，禁止 INSERT/UPDATE/DELETE 等修改操作
2. 生成的 SQL 需适配 PostgreSQL 语法
3. 解释生成的 SQL 逻辑，方便用户理解
4. 如果用户问题不涉及 PostgreSQL 查询，回复「该问题不涉及 PostgreSQL 查询，请输入 PG查询 相关问题」

当前数据库结构信息（仅此部分，禁止扩展）：
{schema_summary}

允许的表和字段清单：
{allowed_tables}

用户问题：{user_input}
"""

    def _get_allowed_tables_and_fields(self) -> str:
        """获取允许的表和字段清单"""
        skill_content = self._skill_data.get("content", "") if self._skill_data else ""
        if not skill_content:
            skill_content = self._get_fallback_skill_content()
        
        schema_info = self._parse_schema_from_skill_content(skill_content)
        
        lines = []
        for table_name, columns in schema_info.items():
            field_names = [col['name'] for col in columns]
            lines.append(f"- {table_name}: {', '.join(field_names)}")
        
        return '\n'.join(lines)

    @property
    def QUERY_TEMPLATES(self) -> Dict[str, str]:
        """查询模板属性"""
        if self._query_templates is None:
            self._query_templates = self._get_default_templates()
        return self._query_templates

    @contextmanager
    def _get_connection(self):
        """获取数据库连接上下文"""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            yield conn
        except psycopg2.Error as e:
            raise Exception(f"数据库连接失败: {e}")
        finally:
            if conn:
                conn.close()

    # ==================== 核心查询方法 ====================

    def execute(self, query_type: str, **kwargs) -> Dict[str, Any]:
        """执行数据库操作"""
        try:
            handler = getattr(self, f"_handle_{query_type}", None)
            if handler:
                return handler(**kwargs)
            elif query_type in self.QUERY_TEMPLATES:
                sql = self._build_query(query_type, **kwargs)
                return self._execute_sql(sql)
            else:
                return {"success": False, "error": f"未知的查询类型: {query_type}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _handle_execute_sql(self, sql: Optional[str] = None, **_) -> Dict[str, Any]:
        """处理原生 SQL 执行"""
        return self._execute_sql(sql)

    def _handle_get_schema(self, table_name: Optional[str] = None, **_) -> Dict[str, Any]:
        """
        处理获取表结构 - 仅从 SKILL.md 返回，不查询数据库
        """
        if not table_name:
            return {"success": False, "error": "表名不能为空"}
        
        # 从 SKILL.md 解析的表结构中查找
        skill_content = self._skill_data.get("content", "") if self._skill_data else ""
        if not skill_content:
            skill_content = self._get_fallback_skill_content()
        
        schema_info = self._parse_schema_from_skill_content(skill_content)
        
        if table_name not in schema_info:
            return {
                "success": False, 
                "error": f"表 '{table_name}' 不在 SKILL.md 定义的表列表中。可用的表: {list(schema_info.keys())}"
            }
        
        return {
            "success": True, 
            "table": table_name, 
            "columns": schema_info[table_name],
            "source": "SKILL.md"
        }

    def _handle_list_tables(self, **_) -> Dict[str, Any]:
        """
        处理列出所有表 - 仅从 SKILL.md 返回，不查询数据库
        """
        # 从 SKILL.md 解析表列表
        skill_content = self._skill_data.get("content", "") if self._skill_data else ""
        if not skill_content:
            skill_content = self._get_fallback_skill_content()
        
        schema_info = self._parse_schema_from_skill_content(skill_content)
        
        tables = []
        for table_name, columns in schema_info.items():
            tables.append({
                "table_name": table_name,
                "column_count": len(columns),
                "source": "SKILL.md"
            })
        
        return {
            "success": True, 
            "tables": tables,
            "note": "表结构来自 SKILL.md 定义，非数据库实时schema"
        }

    def _handle_check_skill(self, user_input: str = "", **_) -> Dict[str, Any]:
        """处理检查 Skill 触发"""
        return {
            "success": True,
            "triggered": self.is_skill_triggered(user_input),
            "triggers": self._skill_data.get("triggers", [])
        }

    def _handle_get_skill_info(self, **_) -> Dict[str, Any]:
        """处理获取 Skill 信息"""
        return {
            "success": True,
            "name": self._skill_data.get("name"),
            "description": self._skill_data.get("description"),
            "triggers": self._skill_data.get("triggers", []),
            "templates": list(self.QUERY_TEMPLATES.keys())
        }

    def get_schema(self) -> Dict[str, Any]:
        """获取技能参数 Schema（实现抽象方法）"""
        return {
            "type": "object",
            "properties": {
                "query_type": {
                    "type": "string",
                    "enum": [
                        "execute_sql", "get_schema", "list_tables",
                        "simple_select", "like_search", "range_query",
                        "inner_join", "left_join", "aggregation", "group_by",
                        "check_skill", "get_skill_info"
                    ],
                    "description": "查询类型"
                },
                "sql": {"type": "string", "description": "原生SQL语句"},
                "table_name": {"type": "string", "description": "表名"},
                "fields": {"type": "string", "description": "查询字段"},
                "condition": {"type": "string", "description": "WHERE条件"},
                "sort_field": {"type": "string", "description": "排序字段"},
                "sort_type": {"type": "string", "enum": ["ASC", "DESC"], "default": "DESC"},
                "page_size": {"type": "integer", "default": 10},
                "page": {"type": "integer", "default": 1},
                "user_input": {"type": "string", "description": "用户输入"}
            },
            "required": ["query_type"]
        }

    def _build_query(self, query_type: str, **params) -> str:
        """根据模板构建查询 SQL"""
        template = self.QUERY_TEMPLATES.get(query_type)
        if not template:
            raise ValueError(f"未知的查询模板: {query_type}")
        
        # 合并默认参数
        merged_params = {**self.TEMPLATE_DEFAULTS, **params}
        
        # 计算 offset
        if "offset" not in merged_params:
            merged_params["offset"] = (merged_params["page"] - 1) * merged_params["page_size"]
        
        # 格式化并清理 SQL
        sql = template.format(**merged_params)
        return self._clean_sql(sql)

    @staticmethod
    def _clean_sql(sql: str) -> str:
        """清理 SQL 语句"""
        # 去除单行注释并压缩空白
        lines = [line for line in sql.split('\n') if not line.strip().startswith('--')]
        return ' '.join(' '.join(lines).split())

    def _validate_sql(self, sql: str) -> bool:
        """验证 SQL 安全性 - 仅允许 SELECT 查询"""
        if not sql:
            return False
        
        # 去除注释和字符串内容
        cleaned = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
        cleaned = re.sub(r"'[^']*'", "''", cleaned)
        cleaned = re.sub(r'"[^"]*"', '""', cleaned)
        cleaned_upper = cleaned.strip().upper()
        
        # 检查禁止的关键字
        if any(kw in cleaned_upper for kw in self.FORBIDDEN_KEYWORDS):
            return False
        
        return cleaned_upper.startswith('SELECT')

    def _execute_sql(self, sql: Optional[str]) -> Dict[str, Any]:
        """执行 SQL 查询"""
        if not sql:
            return {"success": False, "error": "SQL 语句不能为空"}
        
        if not self._validate_sql(sql):
            return {"success": False, "error": "只允许执行 SELECT 查询，禁止修改操作"}
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                results = [self._serialize_row(dict(row)) for row in rows]
                
                return {
                    "success": True,
                    "data": results,
                    "count": len(results),
                    "sql": sql
                }

    # ==================== 数据序列化 ====================

    @staticmethod
    def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
        """序列化行数据"""
        return {k: DatabaseSkill._serialize_value(v) for k, v in row.items()}

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        """序列化单个值，处理特殊类型"""
        if value is None:
            return None
        if isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, (time, timedelta)):
            return str(value)
        if isinstance(value, Decimal):
            return float(value)
        if hasattr(value, '__class__') and value.__class__.__name__ == 'Decimal':
            return float(value)
        return str(value)

    # ==================== 数据库结构查询 ====================

    def _get_table_schema(self, table_name: Optional[str]) -> Dict[str, Any]:
        """获取表结构"""
        if not table_name:
            return {"success": False, "error": "表名不能为空"}
        
        sql = """
            SELECT column_name, data_type, is_nullable, column_default,
                   character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public'
            ORDER BY ordinal_position
        """
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, (table_name,))
                columns = [dict(row) for row in cur.fetchall()]
                return {"success": True, "table": table_name, "columns": columns}

    def _list_tables(self) -> Dict[str, Any]:
        """列出所有表"""
        sql = """
            SELECT table_name, table_type,
                (SELECT COUNT(*) FROM information_schema.columns c 
                 WHERE c.table_name = t.table_name AND c.table_schema = 'public') as column_count
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            ORDER BY table_name
        """
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                tables = [dict(row) for row in cur.fetchall()]
                return {"success": True, "tables": tables}

    def get_database_schema_summary(self) -> str:
        """
        获取数据库结构摘要（用于 Agent 上下文）
        仅从 SKILL.md 文件中定义的表结构获取，不从数据库读取 schema
        """
        try:
            # 从 SKILL.md 内容中解析表结构
            skill_content = self._skill_data.get("content", "") if self._skill_data else ""
            
            if not skill_content:
                # 如果没有加载到 skill 内容，使用默认内容
                skill_content = self._get_fallback_skill_content()
            
            # 解析表结构
            schema_info = self._parse_schema_from_skill_content(skill_content)
            
            if not schema_info:
                return "无法从 SKILL.md 获取表结构定义"
            
            lines = ["数据库表结构（基于 SKILL.md 定义）:"]
            
            for table_name, columns in schema_info.items():
                lines.append(f"\n表: {table_name}")
                for col in columns:
                    lines.append(f"  - {col['name']}: {col['type']} ({col['description']})")
            
            return "\n".join(lines)
        except Exception as e:
            return f"获取数据库结构失败: {e}"
    
    def _parse_schema_from_skill_content(self, content: str) -> Dict[str, List[Dict]]:
        """
        从 SKILL.md 内容中解析表结构定义
        返回格式: {表名: [{name: 字段名, type: 类型, description: 说明}, ...]}
        """
        schema_info = {}
        
        # 按表分割内容，匹配 "### X. 表名（table_name）" 格式
        table_pattern = r'### \d+\.\s*([^（]+)（([^）]+)）\s*\n\|[^\n]+\n\|[-\|]+\n((?:\|[^\n]+\n)+)'
        
        matches = re.findall(table_pattern, content, re.MULTILINE)
        
        for match in matches:
            table_desc = match[0].strip()  # 表描述，如 "销售记录表"
            table_name = match[1].strip()  # 表名，如 "sell_records"
            table_content = match[2]       # 表格内容
            
            columns = []
            # 解析每一行
            for line in table_content.strip().split('\n'):
                line = line.strip()
                if not line.startswith('|'):
                    continue
                
                parts = [p.strip() for p in line.split('|')]
                # 过滤空字符串
                parts = [p for p in parts if p]
                
                if len(parts) >= 3:
                    # 跳过表头行
                    if parts[0] in ['字段名', '---', '--', '-']:
                        continue
                    
                    col_name = parts[0]
                    col_type = parts[1] if len(parts) > 1 else "unknown"
                    col_desc = parts[2] if len(parts) > 2 else ""
                    
                    columns.append({
                        'name': col_name,
                        'type': col_type,
                        'description': col_desc
                    })
            
            if columns:
                schema_info[table_name] = columns
        
        return schema_info

    def get_query_templates_help(self) -> str:
        """获取查询模板帮助文档"""
        templates = "\n".join(f"- {name}" for name in self.QUERY_TEMPLATES.keys())
        triggers = ", ".join(self._skill_data.get("triggers", []))
        
        return f"""可用的PostgreSQL查询模板：

{templates}

Skill 信息：
- 名称: {self._skill_data.get('name', 'unknown')}
- 描述: {self._skill_data.get('description', 'N/A')}
- 触发词: {triggers}

使用规范：
- 所有查询仅使用SELECT语句
- 复杂查询建议先LIMIT 10测试结果
- 联表查询优先使用表别名
- 时间范围查询建议使用 >= + < 组合

新增查询类型：
- check_skill: 检查用户输入是否触发Skill
- get_skill_info: 获取Skill详细信息
"""

    # ==================== LangChain Tool 工厂方法 ====================

    @classmethod
    def create_tools(cls) -> List['Tool']:
        """创建 LangChain Tool 工具列表"""
        if not LANGCHAIN_AVAILABLE:
            raise ImportError("langchain_core.tools 未安装，无法创建 Tool")
        
        instance = cls()
        
        return [
            Tool(
                name="DatabaseQuery",
                func=lambda x: json.dumps(instance.execute(**json.loads(x)), ensure_ascii=False),
                description=cls._get_database_query_description()
            ),
            Tool(
                name="GetDatabaseSchema",
                func=lambda _: instance.get_database_schema_summary(),
                description="""【重要】获取数据库表结构定义（仅来自 SKILL.md 文件）。
此工具返回的表结构完全基于 SKILL.md 中定义的表和字段，不包含数据库中的其他表。
可用的表仅包括：sell_records, sell_details, vip_info, pro_info, weather_history。
生成SQL时必须严格使用这些表中定义的字段，禁止自行推断或使用未定义的字段。"""
            )
        ]

    @classmethod
    def _get_database_query_description(cls) -> str:
        """获取 DatabaseQuery 工具的详细描述"""
        return """用于查询PostgreSQL数据库，基于postgresql-operations skill规范。输入必须是JSON格式:
{
    "query_type": "查询类型",
    "其他参数..."
}

支持的query_type:
- execute_sql: 执行原生SELECT SQL
- get_schema: 获取表结构
- list_tables: 列出所有表
- simple_select: 简单条件查询（参数: table_name, fields, condition, sort_field, page_size, page）
- like_search: 模糊搜索（参数: table_name, field, keyword）
- range_query: 范围查询
- inner_join: 内连接查询
- left_join: 左连接查询
- aggregation: 聚合统计
- group_by: 分组聚合
- check_skill: 检查用户输入是否触发Skill（参数: user_input）
- get_skill_info: 获取Skill详细信息

Skill触发词: PG查询、PostgreSQL查询、pg联表查询、pg聚合查询等

使用示例:
{"query_type": "simple_select", "table_name": "users", "fields": "id,name,email", "condition": "status='active'", "page_size": 10}
{"query_type": "like_search", "table_name": "users", "field": "name", "keyword": "张三"}
{"query_type": "execute_sql", "sql": "SELECT * FROM users LIMIT 10"}
{"query_type": "check_skill", "user_input": "PG查询用户信息"}
"""

    @classmethod
    def get_database_query_tool(cls) -> 'Tool':
        """获取 DatabaseQuery 工具"""
        if not LANGCHAIN_AVAILABLE:
            raise ImportError("langchain_core.tools 未安装，无法创建 Tool")
        
        instance = cls()
        return Tool(
            name="DatabaseQuery",
            func=lambda x: json.dumps(instance.execute(**json.loads(x)), ensure_ascii=False),
            description=cls._get_database_query_description()
        )

    @classmethod
    def get_schema_tool(cls) -> 'Tool':
        """获取 GetDatabaseSchema 工具"""
        if not LANGCHAIN_AVAILABLE:
            raise ImportError("langchain_core.tools 未安装，无法创建 Tool")
        
        instance = cls()
        return Tool(
            name="GetDatabaseSchema",
            func=lambda _: instance.get_database_schema_summary(),
            description="获取数据库整体结构摘要，用于了解有哪些表和字段"
        )
