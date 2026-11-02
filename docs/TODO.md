# 线程池实现 TODO

## 阶段一：基础组件

- [ ] **work_stealing_queue.hpp** - 本地工作窃取队列
  - [ ] push() - LIFO 推入尾部
  - [ ] pop() - LIFO 从尾部弹出（所有者线程调用）
  - [ ] steal() - FIFO 从头部窃取（其他线程调用）
  - [ ] empty() / size() - 线程安全查询
  - [ ] 单元测试：并发 push/pop/steal 正确性

- [ ] **priority_queue.hpp** - 全局优先级队列
  - [ ] 5 级优先级槽位（critical/high/normal/low/background）
  - [ ] push() - 按优先级入队
  - [ ] pop() - 从最高优先级出队
  - [ ] 无锁 CAS 实现（每级独立 atomic 栈）
  - [ ] 老化机制：低优先级任务等待过久自动提升
  - [ ] 单元测试：优先级排序、老化提升

- [ ] **task.hpp** - 任务抽象
  - [ ] task_state 枚举（pending/queued/running/completed/canceled/failed）
  - [ ] task_priority 枚举（5 级）
  - [ ] task_base 虚基类（execute/cancel/状态查询/时间信息/回调）
  - [ ] task<R> 模板子类（packaged_task 包装，返回 std::future）
  - [ ] any_task 类型擦除包装（用于队列存储）
  - [ ] 单元测试：状态流转、取消、异常传播

## 阶段二：核心线程池

- [ ] **thread_manager.hpp** - 线程生命周期管理
  - [ ] 核心线程创建与常驻
  - [ ] 动态线程创建（扩展触发：队列积压 > 线程数 × 阈值）
  - [ ] 动态线程回收（收缩触发：活跃线程 < 总线程 × 0.3，空闲超时）
  - [ ] thread_stats 单线程统计（执行任务数/等待时间/活跃状态）
  - [ ] config 结构体（core_threads/max_threads/idle_timeout/growth_factor/shrink_factor）
  - [ ] 单元测试：伸缩逻辑、超时回收

- [ ] **statistics.hpp** - 运行时统计
  - [ ] atomic 计数器：tasks_submitted/completed/canceled/failed
  - [ ] peak_queue_size 追踪
  - [ ] avg_wait_time_ms 计算
  - [ ] start_time 记录
  - [ ] 单元测试：并发计数正确性

- [ ] **thread_pool.hpp** - 主入口
  - [ ] 构造/析构（启动核心线程、Manager 线程、优雅关闭）
  - [ ] worker_loop() - 工作线程主循环（本地队列 → 全局队列 → 窃取 → 等待）
  - [ ] manager_loop() - 管理线程循环（监控负载、触发伸缩）
  - [ ] 任务分发：submit_to_queue()（优先级路由）
  - [ ] 工作窃取：try_steal_work()（随机 victim、指数退避）
  - [ ] 析构：drain 剩余任务、join 所有线程
  - [ ] 集成测试：基本提交/执行/关闭流程

## 阶段三：高级功能

- [ ] 任务提交接口
  - [ ] submit(f, args...) → std::future<R>
  - [ ] submit_priority(p, f, args...) → std::future<R>
  - [ ] submit_delayed(delay, f, args...) → std::future<R>（延迟队列 + 定时转移）
  - [ ] submit_batch(f, count) → vector<future<R>>
  - [ ] detach(f, args...) → void（fire-and-forget）

- [ ] 任务控制
  - [ ] wait() - 等待所有任务完成
  - [ ] wait_for(timeout) - 带超时等待
  - [ ] cancel_pending() - 取消所有排队任务
  - [ ] pause() / resume() - 暂停/恢复
  - [ ] 任务级取消（cancel 标志位 + 检查点模式）

- [ ] 线程控制
  - [ ] set_thread_count(n) - 手动调整线程数
  - [ ] enable_scaling(flag) - 启用/禁用动态伸缩
  - [ ] thread_count() - 查询当前线程数

- [ ] 集成测试：高级功能组合验证

## 阶段四：测试与优化

- [ ] 单元测试
  - [ ] 并发提交：多线程同时 submit 不丢失
  - [ ] 优先级：高优先级任务先执行
  - [ ] 取消：未执行任务正确取消
  - [ ] 异常：异常正确传播到 future
  - [ ] 延迟：延迟任务按时触发

- [ ] 集成测试
  - [ ] 工作窃取：空闲线程成功窃取
  - [ ] 动态伸缩：负载变化时线程数正确调整
  - [ ] 暂停/恢复：pause 后不取任务，resume 后恢复
  - [ ] 资源清理：析构时所有任务完成或取消，无泄漏

- [ ] 性能测试
  - [ ] 吞吐量：简单任务 > 1M tasks/sec
  - [ ] 延迟：入队延迟 < 10μs
  - [ ] 内存：每任务开销 < 1KB
  - [ ] 对比基准：与现有 ThreadPool.hpp / thread_pool.hpp 对比

- [ ] 优化
  - [ ] 自旋 vs 阻塞策略（短等待 yield，长等待 cv.wait）
  - [ ] 批量窃取（一次取多个任务减少竞争）
  - [ ] 缓存行对齐（避免 false sharing）
