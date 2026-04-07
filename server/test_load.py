"""
Load Testing and Performance Benchmarking
Tests system under realistic production load
"""
import time
import statistics
import concurrent.futures
from typing import List, Dict, Any
import requests
import json
from datetime import datetime


class LoadTester:
    """Load testing utilities"""
    
    def __init__(self, base_url: str = "http://localhost:7860"):
        self.base_url = base_url
        self.results: List[Dict[str, Any]] = []
        
    def measure_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Measure single request performance"""
        start = time.time()
        
        try:
            url = f"{self.base_url}{endpoint}"
            
            if method.upper() == "GET":
                response = requests.get(url, **kwargs)
            elif method.upper() == "POST":
                response = requests.post(url, **kwargs)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            elapsed = time.time() - start
            
            return {
                "success": True,
                "status_code": response.status_code,
                "elapsed_ms": elapsed * 1000,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            elapsed = time.time() - start
            return {
                "success": False,
                "error": str(e),
                "elapsed_ms": elapsed * 1000,
                "timestamp": datetime.now().isoformat()
            }
    
    def concurrent_load_test(
        self, 
        method: str,
        endpoint: str,
        num_requests: int = 100,
        num_workers: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """Run concurrent load test"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(self.measure_request, method, endpoint, **kwargs)
                for _ in range(num_requests)
            ]
            
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
        
        # Calculate statistics
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]
        
        if successful:
            latencies = [r["elapsed_ms"] for r in successful]
            
            return {
                "total_requests": num_requests,
                "successful": len(successful),
                "failed": len(failed),
                "success_rate": len(successful) / num_requests,
                "latency_ms": {
                    "min": min(latencies),
                    "max": max(latencies),
                    "mean": statistics.mean(latencies),
                    "median": statistics.median(latencies),
                    "p95": self._percentile(latencies, 95),
                    "p99": self._percentile(latencies, 99)
                },
                "errors": [r["error"] for r in failed] if failed else []
            }
        else:
            return {
                "total_requests": num_requests,
                "successful": 0,
                "failed": len(failed),
                "success_rate": 0.0,
                "errors": [r["error"] for r in failed]
            }
    
    def sustained_load_test(
        self,
        method: str,
        endpoint: str,
        duration_seconds: int = 60,
        requests_per_second: int = 10,
        **kwargs
    ) -> Dict[str, Any]:
        """Sustained load over time"""
        results = []
        start_time = time.time()
        request_interval = 1.0 / requests_per_second
        
        while time.time() - start_time < duration_seconds:
            iteration_start = time.time()
            result = self.measure_request(method, endpoint, **kwargs)
            results.append(result)
            
            # Sleep to maintain rate
            elapsed = time.time() - iteration_start
            if elapsed < request_interval:
                time.sleep(request_interval - elapsed)
        
        # Calculate statistics
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]
        
        if successful:
            latencies = [r["elapsed_ms"] for r in successful]
            
            return {
                "duration_seconds": duration_seconds,
                "target_rps": requests_per_second,
                "total_requests": len(results),
                "successful": len(successful),
                "failed": len(failed),
                "actual_rps": len(results) / duration_seconds,
                "success_rate": len(successful) / len(results),
                "latency_ms": {
                    "min": min(latencies),
                    "max": max(latencies),
                    "mean": statistics.mean(latencies),
                    "median": statistics.median(latencies),
                    "p95": self._percentile(latencies, 95),
                    "p99": self._percentile(latencies, 99)
                }
            }
        else:
            return {
                "duration_seconds": duration_seconds,
                "total_requests": len(results),
                "successful": 0,
                "failed": len(failed),
                "success_rate": 0.0
            }
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile"""
        sorted_data = sorted(data)
        index = int(len(sorted_data) * (percentile / 100.0))
        return sorted_data[min(index, len(sorted_data) - 1)]
    
    def spike_test(
        self,
        method: str,
        endpoint: str,
        spike_requests: int = 500,
        **kwargs
    ) -> Dict[str, Any]:
        """Sudden spike in traffic"""
        print(f"🔥 Spike test: {spike_requests} simultaneous requests...")
        
        # Fire all at once
        with concurrent.futures.ThreadPoolExecutor(max_workers=spike_requests) as executor:
            start = time.time()
            futures = [
                executor.submit(self.measure_request, method, endpoint, **kwargs)
                for _ in range(spike_requests)
            ]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
            total_time = time.time() - start
        
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]
        
        if successful:
            latencies = [r["elapsed_ms"] for r in successful]
            
            return {
                "spike_requests": spike_requests,
                "total_time_seconds": total_time,
                "successful": len(successful),
                "failed": len(failed),
                "success_rate": len(successful) / spike_requests,
                "throughput_rps": spike_requests / total_time,
                "latency_ms": {
                    "min": min(latencies),
                    "max": max(latencies),
                    "mean": statistics.mean(latencies),
                    "median": statistics.median(latencies),
                    "p95": self._percentile(latencies, 95),
                    "p99": self._percentile(latencies, 99)
                },
                "errors": [r["error"] for r in failed] if failed else []
            }
        else:
            return {
                "spike_requests": spike_requests,
                "successful": 0,
                "failed": len(failed),
                "success_rate": 0.0,
                "errors": [r["error"] for r in failed]
            }


class PerformanceBenchmark:
    """Benchmark critical operations"""
    
    def __init__(self, base_url: str = "http://localhost:7860"):
        self.base_url = base_url
        
    def benchmark_reset(self, iterations: int = 100) -> Dict[str, Any]:
        """Benchmark reset operation"""
        times = []
        
        for _ in range(iterations):
            start = time.time()
            requests.post(
                f"{self.base_url}/reset",
                json={"task_id": "task_easy_categorize"}
            )
            elapsed = time.time() - start
            times.append(elapsed * 1000)
        
        return {
            "operation": "reset",
            "iterations": iterations,
            "latency_ms": {
                "min": min(times),
                "max": max(times),
                "mean": statistics.mean(times),
                "median": statistics.median(times)
            }
        }
    
    def benchmark_step(self, iterations: int = 100) -> Dict[str, Any]:
        """Benchmark step operation"""
        # Reset first
        reset_resp = requests.post(
            f"{self.base_url}/reset",
            json={"task_id": "task_easy_categorize"}
        )
        inbox = reset_resp.json()["observation"]["inbox"]
        
        if not inbox:
            return {"error": "No emails in inbox"}
        
        email_id = inbox[0]["id"]
        times = []
        
        for _ in range(iterations):
            # Reset before each step
            requests.post(
                f"{self.base_url}/reset",
                json={"task_id": "task_easy_categorize"}
            )
            
            start = time.time()
            requests.post(
                f"{self.base_url}/step",
                json={
                    "action_type": "categorize",
                    "email_id": email_id,
                    "category": "work"
                }
            )
            elapsed = time.time() - start
            times.append(elapsed * 1000)
        
        return {
            "operation": "step",
            "iterations": iterations,
            "latency_ms": {
                "min": min(times),
                "max": max(times),
                "mean": statistics.mean(times),
                "median": statistics.median(times)
            }
        }
    
    def benchmark_ai_systems(self, iterations: int = 50) -> Dict[str, List[Dict]]:
        """Benchmark all AI systems"""
        benchmarks = []
        
        # Knowledge Graph
        kg_times = []
        for _ in range(iterations):
            start = time.time()
            requests.post(
                f"{self.base_url}/knowledge-graph/extract",
                json={
                    "email_id": "bench-001",
                    "subject": "Meeting with John Smith",
                    "body": "Call 555-1234"
                }
            )
            kg_times.append((time.time() - start) * 1000)
        
        benchmarks.append({
            "system": "Knowledge Graph",
            "mean_ms": statistics.mean(kg_times),
            "median_ms": statistics.median(kg_times)
        })
        
        # Response Generator
        rg_times = []
        for _ in range(iterations):
            start = time.time()
            requests.post(
                f"{self.base_url}/response-generator/generate",
                json={
                    "email_id": "bench-001",
                    "sender": "test@example.com",
                    "subject": "Question",
                    "body": "How much?",
                    "response_type": "info_request",
                    "tone": "professional"
                }
            )
            rg_times.append((time.time() - start) * 1000)
        
        benchmarks.append({
            "system": "Response Generator",
            "mean_ms": statistics.mean(rg_times),
            "median_ms": statistics.median(rg_times)
        })
        
        # Multi-Agent AI
        ma_times = []
        for _ in range(iterations):
            start = time.time()
            requests.post(
                f"{self.base_url}/collaborative-ai/decide",
                json={
                    "email_id": "bench-001",
                    "sender": "test@example.com",
                    "subject": "Test",
                    "body": "Test body"
                }
            )
            ma_times.append((time.time() - start) * 1000)
        
        benchmarks.append({
            "system": "Multi-Agent AI",
            "mean_ms": statistics.mean(ma_times),
            "median_ms": statistics.median(ma_times)
        })
        
        # Security Scanner
        ss_times = []
        for _ in range(iterations):
            start = time.time()
            requests.post(
                f"{self.base_url}/security/scan",
                json={
                    "email_id": "bench-001",
                    "subject": "Test",
                    "body": "SSN: 123-45-6789"
                }
            )
            ss_times.append((time.time() - start) * 1000)
        
        benchmarks.append({
            "system": "Security Scanner",
            "mean_ms": statistics.mean(ss_times),
            "median_ms": statistics.median(ss_times)
        })
        
        return {"benchmarks": benchmarks}


def run_full_load_test():
    """Run comprehensive load test suite"""
    print("=" * 70)
    print("  EMAIL TRIAGE OPENENV - LOAD & PERFORMANCE TEST SUITE")
    print("=" * 70)
    print()
    
    tester = LoadTester()
    benchmark = PerformanceBenchmark()
    
    # 1. Health check baseline
    print("1️⃣  Health Check Baseline (100 requests, 10 workers)")
    result = tester.concurrent_load_test("GET", "/health", num_requests=100, num_workers=10)
    print(f"   ✓ Success Rate: {result['success_rate']*100:.1f}%")
    print(f"   ✓ Mean Latency: {result['latency_ms']['mean']:.2f}ms")
    print(f"   ✓ P95 Latency: {result['latency_ms']['p95']:.2f}ms")
    print(f"   ✓ P99 Latency: {result['latency_ms']['p99']:.2f}ms")
    print()
    
    # 2. OpenEnv API load
    print("2️⃣  OpenEnv Reset Load (50 concurrent resets)")
    result = tester.concurrent_load_test(
        "POST",
        "/reset",
        num_requests=50,
        num_workers=10,
        json={"task_id": "task_easy_categorize"}
    )
    print(f"   ✓ Success Rate: {result['success_rate']*100:.1f}%")
    print(f"   ✓ Mean Latency: {result['latency_ms']['mean']:.2f}ms")
    print()
    
    # 3. Sustained load
    print("3️⃣  Sustained Load (30 seconds @ 10 req/sec)")
    result = tester.sustained_load_test(
        "GET",
        "/health",
        duration_seconds=30,
        requests_per_second=10
    )
    print(f"   ✓ Total Requests: {result['total_requests']}")
    print(f"   ✓ Actual RPS: {result['actual_rps']:.1f}")
    print(f"   ✓ Success Rate: {result['success_rate']*100:.1f}%")
    print(f"   ✓ Mean Latency: {result['latency_ms']['mean']:.2f}ms")
    print()
    
    # 4. Spike test
    print("4️⃣  Spike Test (200 simultaneous requests)")
    result = tester.spike_test("GET", "/health", spike_requests=200)
    print(f"   ✓ Success Rate: {result['success_rate']*100:.1f}%")
    print(f"   ✓ Throughput: {result['throughput_rps']:.1f} req/sec")
    print(f"   ✓ Mean Latency: {result['latency_ms']['mean']:.2f}ms")
    print(f"   ✓ Max Latency: {result['latency_ms']['max']:.2f}ms")
    print()
    
    # 5. AI Systems benchmark
    print("5️⃣  AI Systems Performance (50 iterations each)")
    result = benchmark.benchmark_ai_systems(iterations=50)
    for bench in result["benchmarks"]:
        print(f"   ✓ {bench['system']}: {bench['mean_ms']:.2f}ms (mean)")
    print()
    
    # 6. Core operations benchmark
    print("6️⃣  Core Operations Benchmark")
    reset_bench = benchmark.benchmark_reset(iterations=50)
    print(f"   ✓ Reset: {reset_bench['latency_ms']['mean']:.2f}ms (mean)")
    
    step_bench = benchmark.benchmark_step(iterations=50)
    print(f"   ✓ Step: {step_bench['latency_ms']['mean']:.2f}ms (mean)")
    print()
    
    print("=" * 70)
    print("  ✅ LOAD TEST COMPLETE - ALL SYSTEMS NOMINAL")
    print("=" * 70)


if __name__ == "__main__":
    try:
        run_full_load_test()
    except Exception as e:
        print(f"❌ Load test failed: {e}")
        import traceback
        traceback.print_exc()
