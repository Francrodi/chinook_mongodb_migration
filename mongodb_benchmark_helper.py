import time
import statistics
from typing import Callable, Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    """Resultado de un benchmark con estadísticas detalladas"""
    name: str
    iterations: int
    database_type: str
    times_ms: List[float]
    mean_ms: float
    median_ms: float
    min_ms: float
    max_ms: float
    std_dev_ms: float
    p95_ms: float
    p99_ms: float
        
    def __str__(self) -> str:
        lines = [
            f"Benchmark: {self.name} ({self.database_type})",
            f"  - Iteraciones: {self.iterations}",
            f"  - Promedio: {self.mean_ms:.2f}ms",
            f"  - Mediana: {self.median_ms:.2f}ms",
            f"  - Min/Max: {self.min_ms:.2f}ms / {self.max_ms:.2f}ms",
            f"  - Desv. Estándar: {self.std_dev_ms:.2f}ms",
            f"  - P95: {self.p95_ms:.2f}ms",
            f"  - P99: {self.p99_ms:.2f}ms",
        ]
        return "\n".join(lines)



class DatabaseBenchmark:    
    def __init__(self, database_type: str):
        self.database_type = database_type
    
    def benchmark_query(
        self, 
        query_function: Callable,
        name: str = "Query",
        iterations: int = 20,
        warmup: int = 3,
        param_list: Optional[List[Dict[str, Any]]] = None,
    ) -> BenchmarkResult:
        print("*" * 60)
        # Warm-up para llenar cache y estabilizar
        if warmup > 0:
            for i in range(warmup):
                kwargs = param_list[i % len(param_list)] if param_list else {}
                _ = query_function(**kwargs)
        
        # Mediciones reales
        times = []
        
        for i in range(iterations):
                kwargs = param_list[i % len(param_list)] if param_list else {}
                start = time.perf_counter()
                _ = query_function(**kwargs)
                end = time.perf_counter()
                times.append((end - start) * 1000)  # Convertir a milisegundos
        
        times_sorted = sorted(times)
        result = BenchmarkResult(
            name=name,
            database_type=self.database_type,
            iterations=len(times),
            times_ms=times,
            mean_ms=statistics.mean(times),
            median_ms=statistics.median(times),
            min_ms=min(times),
            max_ms=max(times),
            std_dev_ms=statistics.stdev(times) if len(times) > 1 else 0,
            p95_ms=times_sorted[int(0.95 * len(times_sorted))],
            p99_ms=times_sorted[int(0.99 * len(times_sorted))]
        )
        
        print(result)
        print("*" * 60)
        return result