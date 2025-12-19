# ES-Issue
关于es并发_delete_by_query删除的问题记录

## 背景
java版本为8，for循环多次执行 ES 的 delete_by_query 操作，query参数相同，导致es端出现409冲突。
ElasticSearch 版本为 8.14.2 ，配置文件参考 script/elasticsearch.yml

## 问题
当循环执行 3 次时，这 3 个操作几乎是并发（或极短时间内连续）发生的：
线程 A (第1次循环): 发起 _delete_by_query 请求。
线程 A (第2次循环): 紧接着发起 _delete_by_query 请求。
ES 端:
第 1 个 _delete_by_query 正在执行，标记文档为删除，版本号增加。
第 2 个 _delete_by_query 到达，发现文档版本号变了（或者正在被修改），触发乐观锁冲突 -> 409 Conflict。

## 问题复现

本项目已配置为 Maven 项目，并包含复现代码。
**注意**：本项目已升级使用 Elasticsearch Java API Client 8.x 以适配较新的 ES 服务端。

### 运行步骤
1. 确保本地运行 Elasticsearch (localhost:9200)。
2. 直接在 IDE 中运行 `com.example.ElasticsearchDemo` 的 `main` 方法。
   或者使用 Maven: `mvn clean compile exec:java -Dexec.mainClass="com.example.ElasticsearchDemo"`

### 代码逻辑
`ElasticsearchDemo.java` 包含两种复现模式：
1. **异步并发 (Async)**: 使用 `deleteByQuery` 的异步方法。
2. **同步并发 (Sync Concurrent)**: 使用多线程并发调用同步的 `deleteByQuery` 方法。

### 现象分析
`_delete_by_query` 并非原子操作，其内部执行流程为：**Scroll (Search) -> Bulk Delete**。
当多个请求并发执行时：
1.  请求 A 和 请求 B 几乎同时执行 Search 阶段，获取到相同的文档快照（包含相同的 `_seq_no` 和 `_primary_term`）。
2.  请求 A 先执行 Delete，文档版本更新（或被标记删除）。
3.  请求 B 随后尝试使用旧的版本号删除同一文档，ES 检测到版本不一致，抛出 `409 Version Conflict`。

## 解决方案

### 1. 允许冲突继续 (推荐)
如果业务逻辑仅要求"删除匹配的文档"，而不关心具体的删除过程，可以在请求中设置 `conflicts("proceed")`。这样 ES 会忽略版本冲突，继续执行删除（或跳过已删除的文档）。

**代码示例 (Java API Client 8.x):**
```java
import co.elastic.clients.elasticsearch._types.Conflicts;

DeleteByQueryRequest request = new DeleteByQueryRequest.Builder()
        .index("posts")
        .query(q -> q.term(t -> t.field("user").value("kimchy")))
        .conflicts(Conflicts.Proceed) // 关键配置：忽略版本冲突
        .build();
```

### 2. 串行执行
避免并发调用 `_delete_by_query`。确保上一个删除请求完成后（`wait_for_completion=true`），再执行下一个。

### 3. 业务层去重
在发起删除请求前，在业务层或应用层加锁，确保同一时间只有一个针对特定条件的删除请求在执行。

