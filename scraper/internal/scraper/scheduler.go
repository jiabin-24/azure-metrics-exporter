package scraper

import (
	"context"
	"sync"
	"time"
)

type scheduledTask struct {
	task    Task
	cancel  context.CancelFunc
	done    chan struct{}
	running bool
}

type scrapeResult struct {
	task  *scheduledTask
	batch *Batch
	err   error
	start time.Time
}

// 相同 interval 的本地任务共享汇聚进度，不等待其他周期或其他实例的任务。
type scrapeGroup struct {
	interval  time.Duration
	next      time.Time
	tasks     map[string]*scheduledTask
	completed map[string]bool
	pending   []Batch
	flushed   bool
}

func (s *Scraper) runBufferedTasks(ctx context.Context, tasks []Task) {
	// 每个任务最多一个在途请求；退出时结果仍可入队，避免等待采集退出时死锁。
	results := make(chan scrapeResult, len(tasks))
	groups := make(map[time.Duration]*scrapeGroup)
	assigned := make(map[string]*scheduledTask)
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
		if !group.flushed && len(group.completed) == len(group.tasks) {
			flush(group)
		}
	}
	start := func(task *scheduledTask) {
		// 上次采集尚未结束时跳过本次调度，不启动同一任务的重叠请求。
		if task.running || ctx.Err() != nil {
			return
		}
		taskCtx, cancel := context.WithCancel(ctx)
		task.cancel, task.done, task.running = cancel, make(chan struct{}), true
		done := task.done
		workers.Go(func() {
			defer close(done)
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
		// 归属变更前的请求仍可贡献数据，但不能更新同 ID 新任务的运行状态。
		if assigned[task.task.ID] == task {
			task.running = false
			group.completed[task.task.ID] = true
		}
		if result.err != nil && ctx.Err() == nil {
			s.Logger.Error("scrape failed", "job", task.task.Job.Name, "target", task.task.Target,
				"workspace", workspaceFor(task.task.Job), "duration", time.Since(result.start), "error", result.err)
		}
		flushCompleted(group)
	}
	reconcile := func() {
		// 只增删本实例归属发生变化的任务，保留其余任务的在途请求和调度进度。
		members := []string(nil)
		if s.Cluster != nil {
			members = s.Cluster.Members()
		}
		wanted := make(map[string]Task)
		for _, task := range tasks {
			if s.Cluster == nil || (s.Cluster.Ready() && taskOwner(task.ID, members) == s.Cluster.Name()) {
				wanted[task.ID] = task
			}
		}
		for id, task := range assigned {
			if _, keep := wanted[id]; !keep && task.running {
				task.cancel()
			}
		}
		for id, task := range assigned {
			if _, keep := wanted[id]; keep {
				continue
			}
			for task.running {
				// 等待移出任务退出时继续接收结果，避免结果通道满后双方互相等待。
				select {
				case <-task.done:
					task.running = false
				case result := <-results:
					accept(result)
				}
			}
			group := groups[time.Duration(task.task.Job.Interval)]
			delete(group.tasks, id)
			delete(group.completed, id)
			delete(assigned, id)
		}
		for id, task := range wanted {
			if _, exists := assigned[id]; exists {
				continue
			}
			interval := time.Duration(task.Job.Interval)
			group := groups[interval]
			if group == nil {
				group = &scrapeGroup{
					interval: interval, next: time.Now().Add(interval),
					tasks: make(map[string]*scheduledTask), completed: make(map[string]bool),
				}
				groups[interval] = group
			}
			runner := &scheduledTask{task: task}
			assigned[id], group.tasks[id] = runner, runner
			group.flushed = false
			start(runner)
		}
		for _, group := range groups {
			flushCompleted(group)
		}
		if s.Cluster == nil {
			s.Logger.Info("standalone tasks assigned", "tasks", len(assigned))
		} else {
			s.Logger.Info("cluster tasks assigned", "node", s.Cluster.Name(),
				"members", members, "ready", s.Cluster.Ready(), "tasks", len(assigned))
		}
	}
	reconcile()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var changes <-chan struct{}
	if s.Cluster != nil {
		changes = s.Cluster.Changes()
	}
	for ctx.Err() == nil {
		now := time.Now()
		wait := time.Hour
		for _, group := range groups {
			if !now.Before(group.next) {
				// 周期截止只发送已有数据，不取消慢请求；晚到结果留给下一批。
				flush(group)
				group.completed = make(map[string]bool)
				group.flushed = false
				// 沿原周期推进并跳过已错过的时刻，避免补跑积压请求或累计时间漂移。
				group.next = group.next.Add((now.Sub(group.next)/group.interval + 1) * group.interval)
				for _, task := range group.tasks {
					start(task)
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
		case <-changes:
			reconcile()
		case result := <-results:
			accept(result)
		case output <- next:
			queued[0] = nil
			queued = queued[1:]
		case <-timer.C:
		}
	}
	// 退出时才取消在途采集；等待其退出并收齐已产生的结果后，再汇聚剩余数据。
	for _, task := range assigned {
		if task.running {
			task.cancel()
		}
	}
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
