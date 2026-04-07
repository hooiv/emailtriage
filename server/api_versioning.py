"""
API Versioning System
Support for multiple API versions with deprecation and migration
"""
import re
import functools
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger("api_versioning")


class VersionStatus(Enum):
    """API version status"""
    CURRENT = "current"       # Active and recommended
    SUPPORTED = "supported"   # Active but not recommended
    DEPRECATED = "deprecated" # Still working but will be removed
    RETIRED = "retired"       # No longer available


@dataclass
class APIVersion:
    """API version definition"""
    version: str
    status: VersionStatus
    release_date: datetime
    deprecation_date: Optional[datetime] = None
    retirement_date: Optional[datetime] = None
    description: str = ""
    breaking_changes: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status.value,
            "release_date": self.release_date.isoformat(),
            "deprecation_date": self.deprecation_date.isoformat() if self.deprecation_date else None,
            "retirement_date": self.retirement_date.isoformat() if self.retirement_date else None,
            "description": self.description,
            "breaking_changes": self.breaking_changes
        }


@dataclass
class EndpointVersion:
    """Versioned endpoint definition"""
    path: str
    method: str
    version: str
    handler: Callable
    deprecated: bool = False
    deprecated_message: str = ""
    replacement_endpoint: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "method": self.method,
            "version": self.version,
            "deprecated": self.deprecated,
            "deprecated_message": self.deprecated_message,
            "replacement_endpoint": self.replacement_endpoint
        }


@dataclass
class MigrationRule:
    """Request/response migration rule"""
    from_version: str
    to_version: str
    field: str
    action: str  # rename, transform, remove, add
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    transformer: Optional[Callable] = None
    
    def apply_to_request(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply migration to request data"""
        result = data.copy()
        
        if self.action == "rename" and self.old_value in result:
            result[self.new_value] = result.pop(self.old_value)
        
        elif self.action == "transform" and self.field in result:
            if self.transformer:
                result[self.field] = self.transformer(result[self.field])
        
        elif self.action == "remove" and self.field in result:
            del result[self.field]
        
        elif self.action == "add" and self.field not in result:
            result[self.field] = self.new_value
        
        return result
    
    def apply_to_response(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply migration to response data (reverse direction)"""
        result = data.copy()
        
        if self.action == "rename" and self.new_value in result:
            result[self.old_value] = result.pop(self.new_value)
        
        elif self.action == "add" and self.field in result:
            # Remove field that was added for newer version
            del result[self.field]
        
        return result


class APIVersionManager:
    """
    Production API Versioning System
    
    Features:
    - Multiple API versions (v1, v2, etc.)
    - Version detection (URL, header, query param)
    - Automatic migration between versions
    - Deprecation warnings
    - Version analytics
    """
    
    def __init__(self):
        self._versions: Dict[str, APIVersion] = {}
        self._endpoints: Dict[str, List[EndpointVersion]] = {}
        self._migrations: Dict[str, List[MigrationRule]] = {}
        self._version_usage: Dict[str, int] = {}
        self._endpoint_usage: Dict[str, int] = {}
        
        # Register default versions
        self._register_default_versions()
        
        logger.info("API Version Manager initialized")
    
    def _register_default_versions(self):
        """Register default API versions"""
        self.register_version(APIVersion(
            version="v1",
            status=VersionStatus.SUPPORTED,
            release_date=datetime(2024, 1, 1),
            description="Initial API version",
            breaking_changes=[]
        ))
        
        self.register_version(APIVersion(
            version="v2",
            status=VersionStatus.CURRENT,
            release_date=datetime(2024, 6, 1),
            description="Current API version with enhanced features",
            breaking_changes=[
                "Changed email response format",
                "Added required 'priority' field",
                "Renamed 'category' to 'classification'"
            ]
        ))
    
    def register_version(self, version: APIVersion):
        """Register an API version"""
        self._versions[version.version] = version
        self._version_usage[version.version] = 0
        logger.info(f"Registered API version: {version.version}")
    
    def register_endpoint(
        self,
        path: str,
        method: str,
        version: str,
        handler: Callable,
        deprecated: bool = False,
        deprecated_message: str = "",
        replacement_endpoint: str = ""
    ):
        """Register a versioned endpoint"""
        endpoint = EndpointVersion(
            path=path,
            method=method.upper(),
            version=version,
            handler=handler,
            deprecated=deprecated,
            deprecated_message=deprecated_message,
            replacement_endpoint=replacement_endpoint
        )
        
        key = f"{method.upper()}:{path}"
        if key not in self._endpoints:
            self._endpoints[key] = []
        
        self._endpoints[key].append(endpoint)
        self._endpoint_usage[f"{version}:{key}"] = 0
    
    def register_migration(self, migration: MigrationRule):
        """Register a migration rule"""
        key = f"{migration.from_version}->{migration.to_version}"
        if key not in self._migrations:
            self._migrations[key] = []
        
        self._migrations[key].append(migration)
    
    def get_version(self, version: str) -> Optional[APIVersion]:
        """Get version info"""
        return self._versions.get(version)
    
    def get_current_version(self) -> str:
        """Get current API version"""
        for v, info in self._versions.items():
            if info.status == VersionStatus.CURRENT:
                return v
        return "v2"  # Default
    
    def detect_version(
        self,
        url: str = None,
        headers: Dict[str, str] = None,
        query_params: Dict[str, str] = None
    ) -> str:
        """Detect API version from request"""
        # Check URL path first (e.g., /api/v1/emails)
        if url:
            match = re.search(r'/v(\d+)/', url)
            if match:
                version = f"v{match.group(1)}"
                if version in self._versions:
                    return version
        
        # Check header (e.g., X-API-Version: v2)
        if headers:
            header_version = headers.get("X-API-Version") or headers.get("x-api-version")
            if header_version and header_version in self._versions:
                return header_version
        
        # Check query param (e.g., ?api_version=v1)
        if query_params:
            param_version = query_params.get("api_version")
            if param_version and param_version in self._versions:
                return param_version
        
        # Default to current version
        return self.get_current_version()
    
    def get_endpoint(
        self,
        path: str,
        method: str,
        version: str
    ) -> Optional[EndpointVersion]:
        """Get endpoint for version"""
        key = f"{method.upper()}:{path}"
        endpoints = self._endpoints.get(key, [])
        
        # Find exact version match
        for ep in endpoints:
            if ep.version == version:
                return ep
        
        # Fall back to lower versions
        version_num = int(version[1:]) if version.startswith("v") else 1
        for v in range(version_num - 1, 0, -1):
            for ep in endpoints:
                if ep.version == f"v{v}":
                    return ep
        
        return None
    
    def migrate_request(
        self,
        data: Dict[str, Any],
        from_version: str,
        to_version: str
    ) -> Dict[str, Any]:
        """Migrate request data between versions"""
        result = data.copy()
        
        from_num = int(from_version[1:]) if from_version.startswith("v") else 1
        to_num = int(to_version[1:]) if to_version.startswith("v") else 1
        
        if from_num < to_num:
            # Upgrade path
            for v in range(from_num, to_num):
                key = f"v{v}->v{v+1}"
                for migration in self._migrations.get(key, []):
                    result = migration.apply_to_request(result)
        else:
            # Downgrade path
            for v in range(from_num, to_num, -1):
                key = f"v{v-1}->v{v}"
                for migration in self._migrations.get(key, []):
                    result = migration.apply_to_response(result)
        
        return result
    
    def migrate_response(
        self,
        data: Dict[str, Any],
        from_version: str,
        to_version: str
    ) -> Dict[str, Any]:
        """Migrate response data to target version"""
        return self.migrate_request(data, from_version, to_version)
    
    def get_deprecation_warning(
        self,
        version: str,
        endpoint: str = None
    ) -> Optional[str]:
        """Get deprecation warning if applicable"""
        ver = self._versions.get(version)
        if ver and ver.status == VersionStatus.DEPRECATED:
            msg = f"API version {version} is deprecated."
            if ver.retirement_date:
                msg += f" It will be retired on {ver.retirement_date.strftime('%Y-%m-%d')}."
            return msg
        
        # Check specific endpoint
        if endpoint:
            key = endpoint
            for ep_list in self._endpoints.values():
                for ep in ep_list:
                    if ep.path == endpoint and ep.version == version and ep.deprecated:
                        return ep.deprecated_message or f"Endpoint {endpoint} is deprecated."
        
        return None
    
    def track_usage(self, version: str, endpoint: str = None):
        """Track API version/endpoint usage"""
        if version in self._version_usage:
            self._version_usage[version] += 1
        
        if endpoint:
            key = f"{version}:{endpoint}"
            self._endpoint_usage[key] = self._endpoint_usage.get(key, 0) + 1
    
    def version_handler(self, target_version: str = None):
        """Decorator for version-aware handlers"""
        def decorator(func: Callable):
            @functools.wraps(func)
            async def wrapper(request, *args, **kwargs):
                # Detect version
                version = self.detect_version(
                    url=str(request.url),
                    headers=dict(request.headers),
                    query_params=dict(request.query_params)
                )
                
                # Track usage
                self.track_usage(version, request.url.path)
                
                # Add deprecation warning to response headers
                warning = self.get_deprecation_warning(version, request.url.path)
                
                # Execute handler
                response = await func(request, *args, version=version, **kwargs)
                
                # Add headers if Response object
                if warning and hasattr(response, 'headers'):
                    response.headers["X-API-Deprecation-Warning"] = warning
                
                return response
            
            return wrapper
        return decorator
    
    def list_versions(self) -> List[Dict[str, Any]]:
        """List all API versions"""
        return [v.to_dict() for v in self._versions.values()]
    
    def list_endpoints(self, version: str = None) -> List[Dict[str, Any]]:
        """List all endpoints"""
        result = []
        for key, endpoints in self._endpoints.items():
            for ep in endpoints:
                if version is None or ep.version == version:
                    result.append(ep.to_dict())
        return result
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get version analytics"""
        total_requests = sum(self._version_usage.values())
        
        version_stats = {}
        for v, count in self._version_usage.items():
            version_info = self._versions.get(v)
            version_stats[v] = {
                "requests": count,
                "percentage": (count / total_requests * 100) if total_requests > 0 else 0,
                "status": version_info.status.value if version_info else "unknown"
            }
        
        # Top endpoints
        top_endpoints = sorted(
            self._endpoint_usage.items(),
            key=lambda x: x[1],
            reverse=True
        )[:20]
        
        # Deprecated usage
        deprecated_count = sum(
            self._version_usage.get(v, 0)
            for v, info in self._versions.items()
            if info.status == VersionStatus.DEPRECATED
        )
        
        return {
            "total_requests": total_requests,
            "version_count": len(self._versions),
            "endpoint_count": sum(len(eps) for eps in self._endpoints.values()),
            "migration_rules": sum(len(m) for m in self._migrations.values()),
            "by_version": version_stats,
            "top_endpoints": dict(top_endpoints),
            "deprecated_usage": deprecated_count,
            "deprecated_percentage": (deprecated_count / total_requests * 100) if total_requests > 0 else 0
        }


# Global instance
_version_manager: Optional[APIVersionManager] = None


def get_version_manager() -> APIVersionManager:
    """Get global version manager"""
    global _version_manager
    if _version_manager is None:
        _version_manager = APIVersionManager()
    return _version_manager


def v1_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform response to v1 format"""
    result = data.copy()
    
    # V1 compatibility transformations
    if "classification" in result:
        result["category"] = result.pop("classification")
    
    if "priority" in result:
        del result["priority"]  # V1 didn't have priority
    
    return result


def v2_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform response to v2 format"""
    result = data.copy()
    
    # V2 format
    if "category" in result:
        result["classification"] = result.pop("category")
    
    if "priority" not in result:
        result["priority"] = "normal"
    
    return result
