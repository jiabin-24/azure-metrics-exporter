package scraper

import (
	"context"
	"sync"
	"time"
)

type scheduledTask struct {
	task    Task
	running bool
}

type scrapeResult struct {
	task  *scheduledTask
	batch *Batch
	err   error
	start time.Time
}

// 相同 interval 的任务作为整批分配；pending 也保留本实例尚未发送的晚到结果。
type scrapeGroup struct {
	interval  time.Duration
	next      time.Time
	tasks     map[string]*scheduledTask
	completed map[string]bool
	pending   []Batch
	flushed   bool
	active    bool
}

func (s *Scraper) runBufferedTasks(ctx context.Context, tasks []Task) {
	var ownsBatch func(time.Duration, time.Time) bool
	if s.Cluster != nil {
		ownsBatch = func(interval time.Duration, start time.Time) bool {
			owner := ""
			if s.Cluster.Ready() {
				owner = batchOwner(interval, start, s.Cluster.Members())
			}
			s.Logger.Info("cluster batch assigned", "node", s.Cluster.Name(),
				"batch_start", start, "interval", interval, "owner", owner)
			return owner == s.Cluster.Name()
		}
	}
	s.runBufferedTasksWithOwner(ctx, tasks, ownsBatch)
}

func (s *Scraper) runBufferedTasksWithOwner(ctx context.Context, tasks []Task, ownsBatch func(time.Duration, time.Time) bool) {
	// 每个任务最多一个在途请求；退出时结果仍可入队，避免等待采集退出时死锁。
	results := make(chan scrapeResult, len(tasks))
	groups := make(map[time.Duration]*scrapeGroup)
	var workers sync.WaitGroup

	deliveries := make(chan []Batch)
	deliveryDone := make(chan struct{})
	// 发送独立于采集调度；停止采集后仍需发送已收到的数据。
	deliveryCtx, stopDelivery := context.WithCancel(context.Background())
	defer stopDelivery()
	go func() {
		defer close(deliveryDone)
		for batches := range deliveries {
			if err := s.BatchSink(deliveryCtx, batches); err != nil {
				s.Logger.Error("buffered Event Hub delivery failed", "batches", len(batches), "error", err)
			}
		}
	}()
	var queued [][]Batch
	flush := func(group *scrapeGroup) {
		// 切走当前批次，后续晚到结果进入新的 pending，不修改已排队的数据。
		if len(group.pending) > 0 {
			queued = append(queued, group.pending)
			group.pending = nil
		}
		group.flushed = true
	}
	flushCompleted := func(group *scrapeGroup) {
		// 空结果、失败和超时也计入完成；提前发送不改变下一次采集的计划时间。
		if group.active && !group.flushed && len(group.completed) == len(group.tasks) {
			flush(group)
		}
	}
	start := func(task *scheduledTask) {
		// 上次采集尚未结束时跳过本次调度，不启动同一任务的重叠请求。
		if task.running || ctx.Err() != nil {
			return
		}
		taskCtx, cancel := context.WithCancel(ctx)
		task.running = true
		workers.Go(func() {
			defer cancel()
			result := scrapeResult{task: task, start: time.Now()}
			// 仅替换当前请求副本的 Sink：先返回采集结果，由调度器统一汇聚发送。
			collector := *s
			collector.Sink = func(_ context.Context, batch Batch) error {
				result.batch = &batch
				return nil
			}
			result.err = collector.Scrape(taskCtx, task.task.Job, task.task.Target, task.task.Labels)
			results <- result
		})
	}
	accept := func(result scrapeResult) {
		task := result.task
		group := groups[time.Duration(task.task.Job.Interval)]
		if result.batch != nil {
			group.pending = append(group.pending, *result.batch)
		}
		task.running = false
		group.completed[task.task.ID] = true
		if result.err != nil && ctx.Err() == nil {
			s.Logger.Error("scrape failed", "job", task.task.Job.Name, "target", task.task.Target,
				"workspace", workspaceFor(task.task.Job), "duration", time.Since(result.start), "error", result.err)
		}
		flushCompleted(group)
	}
	now := time.Now()
	for _, task := range tasks {
		interval := time.Duration(task.Job.Interval)
		group := groups[interval]
		if group == nil {
			next := now.Add(interval)
			if ownsBatch != nil {
				// 集群启动不补采当前批次，统一等下一时间边界，避免重启或新成员接管半批。
				next = batchStart(now, interval).Add(interval)
			}
			group = &scrapeGroup{
				interval: interval, next: next, active: ownsBatch == nil,
				tasks: make(map[string]*scheduledTask), completed: make(map[string]bool),
			}
			groups[interval] = group
		}
		group.tasks[task.ID] = &scheduledTask{task: task}
	}
	if ownsBatch == nil {
		s.Logger.Info("standalone tasks assigned", "tasks", len(tasks))
		for _, group := range groups {
			for _, task := range group.tasks {
				start(task)
			}
		}
	}
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for ctx.Err() == nil {
		now := time.Now()
		wait := time.Hour
		for _, group := range groups {
			if !now.Before(group.next) {
				// 周期截止只发送已有数据，不取消慢请求；晚到结果留给下一批。
				if group.active {
					flush(group)
				}
				group.completed = make(map[string]bool)
				group.flushed = false
				// 沿原周期推进并跳过已错过的时刻，避免补跑积压请求或累计时间漂移。
				group.next = group.next.Add((now.Sub(group.next)/group.interval + 1) * group.interval)
				// 仅在新批次读取成员视图；成员变化不取消旧请求，也不接管执行中的批次。
				group.active = ownsBatch == nil || ownsBatch(group.interval, group.next.Add(-group.interval))
				if group.active {
					for _, task := range group.tasks {
						start(task)
					}
				}
			}
			if remaining := group.next.Sub(now); remaining < wait {
				wait = remaining
			}
		}
		timer.Reset(wait)
		// 无待发送批次时保持 nil，禁用 select 中的发送分支。
		var output chan<- []Batch
		var next []Batch
		if len(queued) > 0 {
			output, next = deliveries, queued[0]
		}
		select {
		case <-ctx.Done():
		case result := <-results:
			accept(result)
		case output <- next:
			queued[0] = nil
			queued = queued[1:]
		case <-timer.C:
		}
	}
	// 退出时才取消在途采集；等待其退出并收齐已产生的结果后，再汇聚剩余数据。
	workers.Wait()
	for len(results) > 0 {
		accept(<-results)
	}
	for _, group := range groups {
		flush(group)
	}
	// 给正在发送及排队中的批次共用 35 秒退出预算，超时后通过发送上下文取消。
	shutdownTimer := time.AfterFunc(35*time.Second, stopDelivery)
	defer shutdownTimer.Stop()
	for _, batches := range queued {
		deliveries <- batches
	}
	close(deliveries)
	<-deliveryDone
}
