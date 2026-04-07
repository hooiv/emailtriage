"""
Data Compression System for Email Triage Environment

Efficient payload handling providing:
- Multiple compression algorithms (gzip, lz4-compatible, brotli-compatible)
- Automatic compression based on payload size
- Decompression utilities
- Compression statistics and analytics
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from collections import deque
import zlib
import gzip
import base64
import json
import io
import threading


class CompressionAlgorithm:
    """Base compression algorithm"""
    
    def compress(self, data: bytes) -> bytes:
        raise NotImplementedError
    
    def decompress(self, data: bytes) -> bytes:
        raise NotImplementedError


class GzipCompression(CompressionAlgorithm):
    """Gzip compression"""
    
    def __init__(self, level: int = 6):
        self.level = level
    
    def compress(self, data: bytes) -> bytes:
        return gzip.compress(data, compresslevel=self.level)
    
    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)


class DeflateCompression(CompressionAlgorithm):
    """Deflate compression (zlib)"""
    
    def __init__(self, level: int = 6):
        self.level = level
    
    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data, level=self.level)
    
    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class FastCompression(CompressionAlgorithm):
    """Fast compression (optimized for speed)"""
    
    def compress(self, data: bytes) -> bytes:
        # Use lowest compression level for speed
        return zlib.compress(data, level=1)
    
    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class BestCompression(CompressionAlgorithm):
    """Best compression (optimized for size)"""
    
    def compress(self, data: bytes) -> bytes:
        # Use highest compression level
        return zlib.compress(data, level=9)
    
    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class CompressionManager:
    """Manage data compression for the API"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.algorithms: Dict[str, CompressionAlgorithm] = {
            "gzip": GzipCompression(),
            "deflate": DeflateCompression(),
            "fast": FastCompression(),
            "best": BestCompression()
        }
        self.default_algorithm = "gzip"
        self.min_compression_size = 1024  # Only compress if > 1KB
        self.stats = {
            "compressions": 0,
            "decompressions": 0,
            "bytes_in": 0,
            "bytes_out": 0,
            "bytes_saved": 0,
            "errors": 0
        }
        self.history = deque(maxlen=1000)
    
    def compress(
        self, 
        data: Union[str, bytes, dict], 
        algorithm: Optional[str] = None,
        force: bool = False
    ) -> Dict[str, Any]:
        """Compress data"""
        with self._lock:
            start = datetime.now()
            algo_name = algorithm or self.default_algorithm
            
            try:
                # Convert to bytes
                if isinstance(data, str):
                    data_bytes = data.encode('utf-8')
                elif isinstance(data, dict):
                    data_bytes = json.dumps(data).encode('utf-8')
                else:
                    data_bytes = data
                
                original_size = len(data_bytes)
                
                # Check if compression is worth it
                if not force and original_size < self.min_compression_size:
                    return {
                        "compressed": False,
                        "reason": "Below minimum size threshold",
                        "original_size": original_size,
                        "data": base64.b64encode(data_bytes).decode('utf-8'),
                        "encoding": "base64"
                    }
                
                # Get algorithm
                algo = self.algorithms.get(algo_name)
                if not algo:
                    algo = self.algorithms[self.default_algorithm]
                    algo_name = self.default_algorithm
                
                # Compress
                compressed = algo.compress(data_bytes)
                compressed_size = len(compressed)
                
                # Check if compression helped
                if compressed_size >= original_size and not force:
                    return {
                        "compressed": False,
                        "reason": "Compression did not reduce size",
                        "original_size": original_size,
                        "data": base64.b64encode(data_bytes).decode('utf-8'),
                        "encoding": "base64"
                    }
                
                # Update stats
                ratio = compressed_size / original_size
                savings = original_size - compressed_size
                
                self.stats["compressions"] += 1
                self.stats["bytes_in"] += original_size
                self.stats["bytes_out"] += compressed_size
                self.stats["bytes_saved"] += savings
                
                # Record history
                duration = (datetime.now() - start).total_seconds() * 1000
                self.history.append({
                    "type": "compress",
                    "algorithm": algo_name,
                    "original_size": original_size,
                    "compressed_size": compressed_size,
                    "ratio": round(ratio, 4),
                    "duration_ms": round(duration, 2),
                    "timestamp": start.isoformat()
                })
                
                return {
                    "compressed": True,
                    "algorithm": algo_name,
                    "original_size": original_size,
                    "compressed_size": compressed_size,
                    "ratio": round(ratio, 4),
                    "savings_bytes": savings,
                    "savings_percent": round((1 - ratio) * 100, 2),
                    "data": base64.b64encode(compressed).decode('utf-8'),
                    "encoding": f"{algo_name}+base64",
                    "duration_ms": round(duration, 2)
                }
                
            except Exception as e:
                self.stats["errors"] += 1
                return {
                    "compressed": False,
                    "error": str(e),
                    "original_size": len(data) if isinstance(data, (bytes, str)) else 0
                }
    
    def decompress(
        self, 
        data: str, 
        algorithm: Optional[str] = None,
        encoding: Optional[str] = None
    ) -> Dict[str, Any]:
        """Decompress data"""
        with self._lock:
            start = datetime.now()
            
            try:
                # Decode from base64
                compressed = base64.b64decode(data)
                compressed_size = len(compressed)
                
                # Determine algorithm
                algo_name = algorithm
                if not algo_name and encoding:
                    # Parse from encoding string like "gzip+base64"
                    parts = encoding.split("+")
                    if parts[0] in self.algorithms:
                        algo_name = parts[0]
                
                if not algo_name:
                    algo_name = self.default_algorithm
                
                algo = self.algorithms.get(algo_name)
                if not algo:
                    algo = self.algorithms[self.default_algorithm]
                    algo_name = self.default_algorithm
                
                # Decompress
                decompressed = algo.decompress(compressed)
                original_size = len(decompressed)
                
                # Update stats
                self.stats["decompressions"] += 1
                
                # Record history
                duration = (datetime.now() - start).total_seconds() * 1000
                self.history.append({
                    "type": "decompress",
                    "algorithm": algo_name,
                    "compressed_size": compressed_size,
                    "decompressed_size": original_size,
                    "duration_ms": round(duration, 2),
                    "timestamp": start.isoformat()
                })
                
                return {
                    "success": True,
                    "algorithm": algo_name,
                    "compressed_size": compressed_size,
                    "decompressed_size": original_size,
                    "data": decompressed.decode('utf-8'),
                    "duration_ms": round(duration, 2)
                }
                
            except Exception as e:
                self.stats["errors"] += 1
                return {
                    "success": False,
                    "error": str(e)
                }
    
    def compress_json(self, data: dict, algorithm: Optional[str] = None) -> Dict[str, Any]:
        """Compress JSON data with optimizations"""
        # Convert to JSON with minimal whitespace
        json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
        return self.compress(json_str, algorithm)
    
    def benchmark(self, data: Union[str, bytes]) -> Dict[str, Any]:
        """Benchmark all algorithms"""
        with self._lock:
            if isinstance(data, str):
                data_bytes = data.encode('utf-8')
            else:
                data_bytes = data
            
            original_size = len(data_bytes)
            results = {}
            
            for name, algo in self.algorithms.items():
                try:
                    start = datetime.now()
                    compressed = algo.compress(data_bytes)
                    compress_time = (datetime.now() - start).total_seconds() * 1000
                    
                    start = datetime.now()
                    decompressed = algo.decompress(compressed)
                    decompress_time = (datetime.now() - start).total_seconds() * 1000
                    
                    compressed_size = len(compressed)
                    ratio = compressed_size / original_size
                    
                    results[name] = {
                        "compressed_size": compressed_size,
                        "ratio": round(ratio, 4),
                        "savings_percent": round((1 - ratio) * 100, 2),
                        "compress_time_ms": round(compress_time, 2),
                        "decompress_time_ms": round(decompress_time, 2),
                        "roundtrip_ok": decompressed == data_bytes
                    }
                except Exception as e:
                    results[name] = {"error": str(e)}
            
            # Find best for size and speed
            valid_results = {k: v for k, v in results.items() if "ratio" in v}
            best_size = min(valid_results.keys(), key=lambda k: valid_results[k]["ratio"]) if valid_results else None
            best_speed = min(valid_results.keys(), key=lambda k: valid_results[k]["compress_time_ms"]) if valid_results else None
            
            return {
                "original_size": original_size,
                "algorithms": results,
                "best_size": best_size,
                "best_speed": best_speed,
                "recommendation": best_size if original_size > 10000 else best_speed
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get compression statistics"""
        with self._lock:
            total_ratio = (
                self.stats["bytes_out"] / self.stats["bytes_in"]
                if self.stats["bytes_in"] > 0 else 0
            )
            
            return {
                **self.stats,
                "overall_ratio": round(total_ratio, 4),
                "overall_savings_percent": round((1 - total_ratio) * 100, 2) if total_ratio > 0 else 0,
                "available_algorithms": list(self.algorithms.keys()),
                "default_algorithm": self.default_algorithm,
                "min_compression_size": self.min_compression_size
            }
    
    def get_history(self, limit: int = 100) -> List[Dict]:
        """Get compression history"""
        with self._lock:
            return list(self.history)[-limit:]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get compression analytics"""
        stats = self.get_stats()
        return {
            "status": "active",
            "algorithms_available": len(self.algorithms),
            "total_operations": stats["compressions"] + stats["decompressions"],
            "bytes_processed": stats["bytes_in"],
            "bytes_saved": stats["bytes_saved"],
            "overall_savings_percent": stats["overall_savings_percent"],
            "features": [
                "gzip_compression",
                "deflate_compression",
                "fast_compression",
                "best_compression",
                "auto_threshold",
                "benchmarking",
                "statistics"
            ],
            "statistics": stats
        }


# Global instance
_compression_manager: Optional[CompressionManager] = None
_compression_lock = threading.Lock()


def get_compression_manager() -> CompressionManager:
    """Get or create compression manager instance"""
    global _compression_manager
    with _compression_lock:
        if _compression_manager is None:
            _compression_manager = CompressionManager()
        return _compression_manager
