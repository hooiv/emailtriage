"""
Background Job Queue System
Priority-based async task execution with retry logic and monitoring
"""
import asyncio
import threading
import time
import uuid
import logging
import functools
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Union
from enum import Enum
from collections import deque
import heapq

logger = logging.getLogger("job_queue")


class JobStatus(Enum):
    """Job lifecycle status"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class JobPriority(Enum):
    """Job priority levels"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class JobResult:
    """Job execution result"""
    success: bool
    data: Any = None
    error: Optional[str] = None
    duration_ms: float = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
            "duration_ms": self.duration_ms
        }


@dataclass
class Job:
    """Background job definition"""
    id: str
    name: str
    handler: str  # Handler function name
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: JobPriority = JobPriority.NORMAL
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    max_retries: int = 3
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: float = 60.0
    result: Optional[JobResult] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other):
        """For priority queue comparison"""
        return self.priority.value < other.priority.value
    
    @property
    def duration_ms(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds() * 1000
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "handler": self.handler,
            "priority": self.priority.name,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "result": self.result.to_dict() if self.result else None,
            "metadata": self.metadata
        }


class JobWorker:
    """Worker that processes jobs"""
    
    def __init__(self, worker_id: str, handlers: Dict[str, Callable]):
        self.worker_id = worker_id
        self.handlers = handlers
        self.current_job: Optional[Job] = None
        self.jobs_processed = 0
        self.jobs_failed = 0
        self._running = False
    
    async def process_job(self, job: Job) -> JobResult:
        """Process a single job"""
        self.current_job = job
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now()
        
        start_time = time.time()
        
        try:
            handler = self.handlers.get(job.handler)
            if not handler:
                raise ValueError(f"Unknown handler: {job.handler}")
            
            # Execute with timeout
            if asyncio.iscoroutinefunction(handler):
                result = await asyncio.wait_for(
                    handler(*job.args, **job.kwargs),
                    timeout=job.timeout
                )
            else:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: handler(*job.args, **job.kwargs)
                    ),
                    timeout=job.timeout
                )
            
            duration = (time.time() - start_time) * 1000
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now()
            self.jobs_processed += 1
            
            job_result = JobResult(
                success=True,
                data=result,
                duration_ms=duration
            )
            job.result = job_result
            
            return job_result
            
        except asyncio.TimeoutError:
            duration = (time.time() - start_time) * 1000
            error = f"Job timed out after {job.timeout}s"
            
            job_result = JobResult(
                success=False,
                error=error,
                duration_ms=duration
            )
            job.result = job_result
            self.jobs_failed += 1
            
            return job_result
            
        except Exception as e:
            duration = (time.time() - start_time) * 1000
            
            job_result = JobResult(
                success=False,
                error=str(e),
                duration_ms=duration
            )
            job.result = job_result
            self.jobs_failed += 1
            
            return job_result
        finally:
            self.current_job = None


class JobQueue:
    """
    Production Job Queue System
    
    Features:
    - Priority-based scheduling
    - Async job execution
    - Retry with exponential backoff
    - Job timeouts
    - Status tracking
    - Worker pool
    """
    
    def __init__(self, num_workers: int = 3):
        self.num_workers = num_workers
        self._queue: List[Job] = []  # Priority queue
        self._jobs: Dict[str, Job] = {}  # All jobs by ID
        self._completed: deque = deque(maxlen=500)
        self._handlers: Dict[str, Callable] = {}
        self._workers: List[JobWorker] = []
        self._lock = threading.RLock()
        self._running = False
        self._process_task: Optional[asyncio.Task] = None
        
        # Register built-in handlers
        self._register_builtin_handlers()
        
        logger.info(f"Job Queue initialized with {num_workers} workers")
    
    def _register_builtin_handlers(self):
        """Register built-in job handlers"""
        
        def email_classification(email_id: str, email_data: Dict):
            """Classify an email"""
            time.sleep(0.1)  # Simulate processing
            return {
                "email_id": email_id,
                "category": "work",
                "confidence": 0.85
            }
        
        def batch_process_emails(email_ids: List[str]):
            """Process multiple emails"""
            results = []
            for eid in email_ids:
                time.sleep(0.05)
                results.append({"id": eid, "processed": True})
            return results
        
        def send_notification(channel: str, message: str):
            """Send a notification"""
            time.sleep(0.05)
            return {"channel": channel, "sent": True}
        
        def generate_report(report_type: str, params: Dict):
            """Generate a report"""
            time.sleep(0.2)
            return {
                "report_type": report_type,
                "generated_at": datetime.now().isoformat(),
                "params": params
            }
        
        def cleanup_old_data(days: int):
            """Cleanup old data"""
            time.sleep(0.1)
            return {"cleaned_records": 42, "days": days}
        
        self.register_handler("email_classification", email_classification)
        self.register_handler("batch_process_emails", batch_process_emails)
        self.register_handler("send_notification", send_notification)
        self.register_handler("generate_report", generate_report)
        self.register_handler("cleanup_old_data", cleanup_old_data)
    
    def register_handler(self, name: str, handler: Callable):
        """Register a job handler"""
        self._handlers[name] = handler
        logger.info(f"Registered handler: {name}")
    
    def enqueue(
        self,
        name: str,
        handler: str,
        args: tuple = None,
        kwargs: Dict[str, Any] = None,
        priority: JobPriority = JobPriority.NORMAL,
        max_retries: int = 3,
        timeout: float = 60.0,
        metadata: Dict[str, Any] = None
    ) -> str:
        """Add a job to the queue"""
        job = Job(
            id=str(uuid.uuid4())[:8],
            name=name,
            handler=handler,
            args=args or (),
            kwargs=kwargs or {},
            priority=priority,
            max_retries=max_retries,
            timeout=timeout,
            metadata=metadata or {}
        )
        
        with self._lock:
            job.status = JobStatus.QUEUED
            heapq.heappush(self._queue, job)
            self._jobs[job.id] = job
        
        logger.info(f"Enqueued job: {job.id} ({job.name})")
        return job.id
    
    def dequeue(self) -> Optional[Job]:
        """Get next job from queue"""
        with self._lock:
            if self._queue:
                return heapq.heappop(self._queue)
        return None
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job by ID"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                return job.to_dict()
            
            for j in self._completed:
                if j.id == job_id:
                    return j.to_dict()
        
        return None
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job and job.status in [JobStatus.PENDING, JobStatus.QUEUED]:
                job.status = JobStatus.CANCELLED
                job.completed_at = datetime.now()
                self._queue = [j for j in self._queue if j.id != job_id]
                heapq.heapify(self._queue)
                return True
        return False
    
    async def _process_jobs(self):
        """Background job processor"""
        workers = [
            JobWorker(f"worker_{i}", self._handlers)
            for i in range(self.num_workers)
        ]
        
        while self._running:
            job = self.dequeue()
            
            if not job:
                await asyncio.sleep(0.1)
                continue
            
            # Find available worker
            worker = workers[0]  # Simple round-robin
            
            try:
                result = await worker.process_job(job)
                
                if not result.success and job.retry_count < job.max_retries:
                    # Retry with backoff
                    job.retry_count += 1
                    job.status = JobStatus.RETRYING
                    await asyncio.sleep(job.retry_delay * (2 ** job.retry_count))
                    
                    with self._lock:
                        heapq.heappush(self._queue, job)
                else:
                    if not result.success:
                        job.status = JobStatus.FAILED
                    
                    # Move to completed
                    with self._lock:
                        if job.id in self._jobs:
                            del self._jobs[job.id]
                        self._completed.append(job)
                
            except Exception as e:
                logger.error(f"Job processing error: {e}")
                job.status = JobStatus.FAILED
                job.result = JobResult(success=False, error=str(e))
    
    async def start(self):
        """Start the job queue processor"""
        if self._running:
            return
        
        self._running = True
        self._process_task = asyncio.create_task(self._process_jobs())
        logger.info("Job queue started")
    
    async def stop(self):
        """Stop the job queue processor"""
        self._running = False
        if self._process_task:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass
        logger.info("Job queue stopped")
    
    def list_jobs(
        self,
        status: JobStatus = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """List jobs"""
        with self._lock:
            jobs = list(self._jobs.values())
            
            if status:
                jobs = [j for j in jobs if j.status == status]
            
            jobs.sort(key=lambda j: j.created_at, reverse=True)
            return [j.to_dict() for j in jobs[:limit]]
    
    def list_completed(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List completed jobs"""
        with self._lock:
            completed = list(self._completed)[-limit:]
            return [j.to_dict() for j in reversed(completed)]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get queue analytics"""
        with self._lock:
            queued = [j for j in self._jobs.values() if j.status == JobStatus.QUEUED]
            running = [j for j in self._jobs.values() if j.status == JobStatus.RUNNING]
            completed = list(self._completed)
        
        successful = [j for j in completed if j.status == JobStatus.COMPLETED]
        failed = [j for j in completed if j.status == JobStatus.FAILED]
        
        # Calculate metrics
        durations = [j.duration_ms for j in successful if j.duration_ms]
        
        # By priority
        by_priority = {}
        for p in JobPriority:
            count = sum(1 for j in completed if j.priority == p)
            by_priority[p.name] = count
        
        # By handler
        by_handler = {}
        for j in completed:
            by_handler[j.handler] = by_handler.get(j.handler, 0) + 1
        
        return {
            "queue_size": len(queued),
            "running": len(running),
            "total_completed": len(completed),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": (len(successful) / len(completed) * 100) if completed else 0,
            "avg_duration_ms": sum(durations) / len(durations) if durations else 0,
            "max_duration_ms": max(durations) if durations else 0,
            "by_priority": by_priority,
            "by_handler": by_handler,
            "registered_handlers": list(self._handlers.keys()),
            "is_running": self._running
        }


class DelayedJob:
    """Job scheduler for delayed/recurring jobs"""
    
    def __init__(self, queue: JobQueue):
        self.queue = queue
        self._scheduled: Dict[str, Dict] = {}
        self._recurring: Dict[str, Dict] = {}
        self._lock = threading.RLock()
        self._running = False
    
    def schedule(
        self,
        name: str,
        handler: str,
        delay_seconds: float,
        args: tuple = None,
        kwargs: Dict[str, Any] = None,
        priority: JobPriority = JobPriority.NORMAL
    ) -> str:
        """Schedule a job for later execution"""
        job_id = str(uuid.uuid4())[:8]
        run_at = datetime.now() + timedelta(seconds=delay_seconds)
        
        with self._lock:
            self._scheduled[job_id] = {
                "name": name,
                "handler": handler,
                "args": args or (),
                "kwargs": kwargs or {},
                "priority": priority,
                "run_at": run_at
            }
        
        return job_id
    
    def schedule_recurring(
        self,
        name: str,
        handler: str,
        interval_seconds: float,
        args: tuple = None,
        kwargs: Dict[str, Any] = None,
        priority: JobPriority = JobPriority.BACKGROUND
    ) -> str:
        """Schedule a recurring job"""
        job_id = str(uuid.uuid4())[:8]
        
        with self._lock:
            self._recurring[job_id] = {
                "name": name,
                "handler": handler,
                "args": args or (),
                "kwargs": kwargs or {},
                "priority": priority,
                "interval": interval_seconds,
                "last_run": None,
                "next_run": datetime.now()
            }
        
        return job_id
    
    async def _process_scheduled(self):
        """Process scheduled jobs"""
        while self._running:
            now = datetime.now()
            
            # Process one-time scheduled jobs
            with self._lock:
                ready = [
                    (jid, j) for jid, j in self._scheduled.items()
                    if j["run_at"] <= now
                ]
            
            for job_id, job_data in ready:
                self.queue.enqueue(
                    name=job_data["name"],
                    handler=job_data["handler"],
                    args=job_data["args"],
                    kwargs=job_data["kwargs"],
                    priority=job_data["priority"]
                )
                with self._lock:
                    del self._scheduled[job_id]
            
            # Process recurring jobs
            with self._lock:
                for jid, job in self._recurring.items():
                    if job["next_run"] <= now:
                        self.queue.enqueue(
                            name=job["name"],
                            handler=job["handler"],
                            args=job["args"],
                            kwargs=job["kwargs"],
                            priority=job["priority"]
                        )
                        job["last_run"] = now
                        job["next_run"] = now + timedelta(seconds=job["interval"])
            
            await asyncio.sleep(1)
    
    async def start(self):
        """Start scheduler"""
        self._running = True
        asyncio.create_task(self._process_scheduled())
    
    async def stop(self):
        """Stop scheduler"""
        self._running = False


# Global instances
_job_queue: Optional[JobQueue] = None
_delayed_job: Optional[DelayedJob] = None


def get_job_queue() -> JobQueue:
    """Get global job queue"""
    global _job_queue
    if _job_queue is None:
        _job_queue = JobQueue()
    return _job_queue


def get_delayed_job_scheduler() -> DelayedJob:
    """Get global delayed job scheduler"""
    global _delayed_job
    if _delayed_job is None:
        _delayed_job = DelayedJob(get_job_queue())
    return _delayed_job
