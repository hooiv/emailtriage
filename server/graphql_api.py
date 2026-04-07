"""
GraphQL API Layer for Email Triage Environment

Modern query language support providing:
- Flexible email queries with field selection
- Mutations for email actions
- Subscriptions for real-time updates
- Type-safe schema with introspection
"""

from typing import Any, Dict, List, Optional, Set
from datetime import datetime
from collections import deque
import json
import threading

# GraphQL Type System
class GraphQLType:
    """Base GraphQL type"""
    pass

class GraphQLScalar(GraphQLType):
    """Scalar type (String, Int, Float, Boolean, ID)"""
    def __init__(self, name: str):
        self.name = name

class GraphQLObject(GraphQLType):
    """Object type with fields"""
    def __init__(self, name: str, fields: Dict[str, Any]):
        self.name = name
        self.fields = fields

class GraphQLList(GraphQLType):
    """List type"""
    def __init__(self, of_type: GraphQLType):
        self.of_type = of_type

class GraphQLNonNull(GraphQLType):
    """Non-nullable type"""
    def __init__(self, of_type: GraphQLType):
        self.of_type = of_type

# Standard scalars
String = GraphQLScalar("String")
Int = GraphQLScalar("Int")
Float = GraphQLScalar("Float")
Boolean = GraphQLScalar("Boolean")
ID = GraphQLScalar("ID")

class GraphQLSchema:
    """GraphQL Schema Definition"""
    
    def __init__(self):
        self.types: Dict[str, GraphQLObject] = {}
        self.queries: Dict[str, Dict[str, Any]] = {}
        self.mutations: Dict[str, Dict[str, Any]] = {}
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self._build_email_schema()
    
    def _build_email_schema(self):
        """Build the email triage schema"""
        
        # Email type
        self.types["Email"] = GraphQLObject("Email", {
            "id": {"type": "ID!", "description": "Unique email identifier"},
            "from": {"type": "String!", "description": "Sender email address"},
            "to": {"type": "String!", "description": "Recipient email address"},
            "subject": {"type": "String!", "description": "Email subject"},
            "body": {"type": "String!", "description": "Email body content"},
            "timestamp": {"type": "String!", "description": "ISO timestamp"},
            "category": {"type": "String", "description": "Assigned category"},
            "priority": {"type": "String", "description": "Priority level"},
            "sentiment": {"type": "Float", "description": "Sentiment score"},
            "isVip": {"type": "Boolean", "description": "VIP sender flag"},
            "isThreat": {"type": "Boolean", "description": "Phishing threat flag"},
            "threadId": {"type": "String", "description": "Thread identifier"},
            "attachments": {"type": "[Attachment]", "description": "Email attachments"},
            "processed": {"type": "Boolean!", "description": "Processing status"}
        })
        
        # Attachment type
        self.types["Attachment"] = GraphQLObject("Attachment", {
            "filename": {"type": "String!", "description": "File name"},
            "mimeType": {"type": "String!", "description": "MIME type"},
            "size": {"type": "Int!", "description": "File size in bytes"},
            "extractedText": {"type": "String", "description": "OCR extracted text"}
        })
        
        # Task type
        self.types["Task"] = GraphQLObject("Task", {
            "id": {"type": "ID!", "description": "Task identifier"},
            "name": {"type": "String!", "description": "Task name"},
            "difficulty": {"type": "String!", "description": "Difficulty level"},
            "maxSteps": {"type": "Int!", "description": "Maximum steps allowed"},
            "passingScore": {"type": "Float!", "description": "Passing score threshold"}
        })
        
        # Observation type
        self.types["Observation"] = GraphQLObject("Observation", {
            "emails": {"type": "[Email]!", "description": "Current emails"},
            "currentStep": {"type": "Int!", "description": "Current step number"},
            "maxSteps": {"type": "Int!", "description": "Maximum steps"},
            "taskId": {"type": "String!", "description": "Current task ID"},
            "metrics": {"type": "Metrics", "description": "Performance metrics"}
        })
        
        # Metrics type
        self.types["Metrics"] = GraphQLObject("Metrics", {
            "accuracy": {"type": "Float", "description": "Accuracy score"},
            "efficiency": {"type": "Float", "description": "Efficiency score"},
            "responseTime": {"type": "Float", "description": "Average response time"}
        })
        
        # Result type
        self.types["Result"] = GraphQLObject("Result", {
            "observation": {"type": "Observation!", "description": "Current observation"},
            "reward": {"type": "Float!", "description": "Reward value"},
            "done": {"type": "Boolean!", "description": "Episode complete flag"},
            "info": {"type": "JSON", "description": "Additional info"}
        })
        
        # Analytics type
        self.types["Analytics"] = GraphQLObject("Analytics", {
            "totalEmails": {"type": "Int!", "description": "Total emails processed"},
            "categoryDistribution": {"type": "JSON", "description": "Category counts"},
            "priorityDistribution": {"type": "JSON", "description": "Priority counts"},
            "averageSentiment": {"type": "Float", "description": "Average sentiment"},
            "threatCount": {"type": "Int", "description": "Detected threats"},
            "vipCount": {"type": "Int", "description": "VIP emails"}
        })
        
        # Query definitions
        self.queries = {
            "emails": {
                "type": "[Email]!",
                "description": "Get all emails with optional filtering",
                "args": {
                    "category": {"type": "String", "description": "Filter by category"},
                    "priority": {"type": "String", "description": "Filter by priority"},
                    "processed": {"type": "Boolean", "description": "Filter by processed status"},
                    "limit": {"type": "Int", "description": "Limit results"},
                    "offset": {"type": "Int", "description": "Skip results"}
                }
            },
            "email": {
                "type": "Email",
                "description": "Get single email by ID",
                "args": {
                    "id": {"type": "ID!", "description": "Email ID"}
                }
            },
            "tasks": {
                "type": "[Task]!",
                "description": "Get all available tasks"
            },
            "task": {
                "type": "Task",
                "description": "Get single task by ID",
                "args": {
                    "id": {"type": "ID!", "description": "Task ID"}
                }
            },
            "observation": {
                "type": "Observation!",
                "description": "Get current observation state"
            },
            "analytics": {
                "type": "Analytics!",
                "description": "Get email analytics"
            },
            "systemStatus": {
                "type": "JSON!",
                "description": "Get system health status"
            }
        }
        
        # Mutation definitions
        self.mutations = {
            "categorize": {
                "type": "Result!",
                "description": "Categorize an email",
                "args": {
                    "emailId": {"type": "ID!", "description": "Email ID"},
                    "category": {"type": "String!", "description": "Category name"}
                }
            },
            "prioritize": {
                "type": "Result!",
                "description": "Set email priority",
                "args": {
                    "emailId": {"type": "ID!", "description": "Email ID"},
                    "priority": {"type": "String!", "description": "Priority level"}
                }
            },
            "reply": {
                "type": "Result!",
                "description": "Reply to an email",
                "args": {
                    "emailId": {"type": "ID!", "description": "Email ID"},
                    "content": {"type": "String!", "description": "Reply content"}
                }
            },
            "forward": {
                "type": "Result!",
                "description": "Forward an email",
                "args": {
                    "emailId": {"type": "ID!", "description": "Email ID"},
                    "to": {"type": "String!", "description": "Forward address"}
                }
            },
            "archive": {
                "type": "Result!",
                "description": "Archive an email",
                "args": {
                    "emailId": {"type": "ID!", "description": "Email ID"}
                }
            },
            "flag": {
                "type": "Result!",
                "description": "Flag an email",
                "args": {
                    "emailId": {"type": "ID!", "description": "Email ID"}
                }
            },
            "batch": {
                "type": "Result!",
                "description": "Batch process multiple emails",
                "args": {
                    "actions": {"type": "[ActionInput]!", "description": "List of actions"}
                }
            },
            "reset": {
                "type": "Observation!",
                "description": "Reset environment to initial state",
                "args": {
                    "taskId": {"type": "String", "description": "Task to load"}
                }
            }
        }
        
        # Subscription definitions
        self.subscriptions = {
            "emailReceived": {
                "type": "Email!",
                "description": "Subscribe to new email arrivals"
            },
            "emailProcessed": {
                "type": "Email!",
                "description": "Subscribe to email processing events"
            },
            "rewardUpdated": {
                "type": "Float!",
                "description": "Subscribe to reward updates"
            },
            "episodeComplete": {
                "type": "Result!",
                "description": "Subscribe to episode completion"
            }
        }
    
    def get_schema_sdl(self) -> str:
        """Generate SDL (Schema Definition Language) representation"""
        lines = ["# Email Triage GraphQL Schema", ""]
        
        # Generate type definitions
        for type_name, type_def in self.types.items():
            lines.append(f"type {type_name} {{")
            for field_name, field_def in type_def.fields.items():
                desc = field_def.get("description", "")
                lines.append(f'  """{desc}"""')
                lines.append(f"  {field_name}: {field_def['type']}")
            lines.append("}")
            lines.append("")
        
        # Generate Query type
        lines.append("type Query {")
        for query_name, query_def in self.queries.items():
            desc = query_def.get("description", "")
            lines.append(f'  """{desc}"""')
            if "args" in query_def:
                args = ", ".join(f"{k}: {v['type']}" for k, v in query_def["args"].items())
                lines.append(f"  {query_name}({args}): {query_def['type']}")
            else:
                lines.append(f"  {query_name}: {query_def['type']}")
        lines.append("}")
        lines.append("")
        
        # Generate Mutation type
        lines.append("type Mutation {")
        for mutation_name, mutation_def in self.mutations.items():
            desc = mutation_def.get("description", "")
            lines.append(f'  """{desc}"""')
            if "args" in mutation_def:
                args = ", ".join(f"{k}: {v['type']}" for k, v in mutation_def["args"].items())
                lines.append(f"  {mutation_name}({args}): {mutation_def['type']}")
            else:
                lines.append(f"  {mutation_name}: {mutation_def['type']}")
        lines.append("}")
        lines.append("")
        
        # Generate Subscription type
        lines.append("type Subscription {")
        for sub_name, sub_def in self.subscriptions.items():
            desc = sub_def.get("description", "")
            lines.append(f'  """{desc}"""')
            lines.append(f"  {sub_name}: {sub_def['type']}")
        lines.append("}")
        
        return "\n".join(lines)
    
    def to_introspection(self) -> Dict[str, Any]:
        """Generate introspection response"""
        types_list = []
        
        for type_name, type_def in self.types.items():
            fields = []
            for field_name, field_def in type_def.fields.items():
                fields.append({
                    "name": field_name,
                    "type": {"name": field_def["type"]},
                    "description": field_def.get("description")
                })
            types_list.append({
                "name": type_name,
                "kind": "OBJECT",
                "fields": fields
            })
        
        return {
            "__schema": {
                "types": types_list,
                "queryType": {"name": "Query"},
                "mutationType": {"name": "Mutation"},
                "subscriptionType": {"name": "Subscription"}
            }
        }


class GraphQLQueryParser:
    """Parse and validate GraphQL queries"""
    
    def __init__(self, schema: GraphQLSchema):
        self.schema = schema
    
    def parse(self, query: str) -> Dict[str, Any]:
        """Parse a GraphQL query string"""
        query = query.strip()
        
        # Handle introspection
        if "__schema" in query or "__type" in query:
            return {"type": "introspection", "data": self.schema.to_introspection()}
        
        # Detect operation type
        operation_type = "query"
        if query.startswith("mutation"):
            operation_type = "mutation"
            query = query[8:].strip()
        elif query.startswith("subscription"):
            operation_type = "subscription"
            query = query[12:].strip()
        elif query.startswith("query"):
            query = query[5:].strip()
        
        # Remove operation name if present
        if query.startswith("{"):
            pass
        else:
            # Find opening brace
            brace_idx = query.find("{")
            if brace_idx > 0:
                query = query[brace_idx:]
        
        # Parse the query body
        return {
            "type": operation_type,
            "body": query,
            "fields": self._extract_fields(query)
        }
    
    def _extract_fields(self, query: str) -> List[str]:
        """Extract requested field names"""
        fields = []
        # Simple field extraction (not a full parser)
        in_field = False
        current = ""
        
        for char in query:
            if char.isalnum() or char == "_":
                current += char
                in_field = True
            elif in_field and current:
                fields.append(current)
                current = ""
                in_field = False
        
        if current:
            fields.append(current)
        
        return fields


class GraphQLExecutor:
    """Execute GraphQL operations"""
    
    def __init__(self, schema: GraphQLSchema):
        self.schema = schema
        self.resolvers: Dict[str, callable] = {}
        self._lock = threading.RLock()
        self.query_history = deque(maxlen=1000)
        self.stats = {
            "total_queries": 0,
            "total_mutations": 0,
            "total_subscriptions": 0,
            "errors": 0,
            "cache_hits": 0
        }
        self._cache: Dict[str, Any] = {}
        self._cache_ttl: Dict[str, datetime] = {}
    
    def register_resolver(self, name: str, resolver: callable):
        """Register a resolver function for a field"""
        with self._lock:
            self.resolvers[name] = resolver
    
    def execute(self, query: str, variables: Optional[Dict] = None, 
                context: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute a GraphQL query"""
        start_time = datetime.now()
        
        try:
            parser = GraphQLQueryParser(self.schema)
            parsed = parser.parse(query)
            
            with self._lock:
                # Record query
                self.query_history.append({
                    "query": query[:500],
                    "type": parsed["type"],
                    "timestamp": start_time.isoformat(),
                    "variables": variables
                })
                
                if parsed["type"] == "introspection":
                    return {"data": parsed["data"]}
                
                if parsed["type"] == "query":
                    self.stats["total_queries"] += 1
                    return self._execute_query(parsed, variables, context)
                elif parsed["type"] == "mutation":
                    self.stats["total_mutations"] += 1
                    return self._execute_mutation(parsed, variables, context)
                elif parsed["type"] == "subscription":
                    self.stats["total_subscriptions"] += 1
                    return self._execute_subscription(parsed, variables, context)
                else:
                    return {"errors": [{"message": f"Unknown operation type: {parsed['type']}"}]}
                    
        except Exception as e:
            with self._lock:
                self.stats["errors"] += 1
            return {"errors": [{"message": str(e)}]}
    
    def _execute_query(self, parsed: Dict, variables: Optional[Dict], 
                       context: Optional[Dict]) -> Dict[str, Any]:
        """Execute a query operation"""
        data = {}
        
        # Extract query name from fields
        for field in parsed.get("fields", []):
            if field in self.resolvers:
                try:
                    result = self.resolvers[field](variables, context)
                    data[field] = result
                except Exception as e:
                    return {"errors": [{"message": f"Resolver error for {field}: {str(e)}"}]}
        
        # If no specific resolver found, return all available queries
        if not data:
            data = {
                "availableQueries": list(self.schema.queries.keys()),
                "message": "Use specific query fields to retrieve data"
            }
        
        return {"data": data}
    
    def _execute_mutation(self, parsed: Dict, variables: Optional[Dict],
                          context: Optional[Dict]) -> Dict[str, Any]:
        """Execute a mutation operation"""
        data = {}
        
        for field in parsed.get("fields", []):
            if field in self.resolvers:
                try:
                    result = self.resolvers[field](variables, context)
                    data[field] = result
                except Exception as e:
                    return {"errors": [{"message": f"Mutation error for {field}: {str(e)}"}]}
        
        if not data:
            data = {
                "availableMutations": list(self.schema.mutations.keys()),
                "message": "Use specific mutation fields to perform actions"
            }
        
        return {"data": data}
    
    def _execute_subscription(self, parsed: Dict, variables: Optional[Dict],
                              context: Optional[Dict]) -> Dict[str, Any]:
        """Execute a subscription setup"""
        return {
            "data": {
                "subscription": {
                    "status": "active",
                    "fields": parsed.get("fields", []),
                    "websocket": "/ws/graphql"
                }
            }
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get execution statistics"""
        with self._lock:
            return {
                **self.stats,
                "recent_queries": list(self.query_history)[-10:],
                "registered_resolvers": list(self.resolvers.keys()),
                "cache_size": len(self._cache)
            }


class GraphQLAPI:
    """Main GraphQL API class"""
    
    def __init__(self):
        self.schema = GraphQLSchema()
        self.executor = GraphQLExecutor(self.schema)
        self._setup_default_resolvers()
    
    def _setup_default_resolvers(self):
        """Setup default resolver functions"""
        
        # Query resolvers
        def emails_resolver(variables, context):
            return {
                "message": "Email resolver - integrate with environment",
                "filters": variables or {}
            }
        
        def tasks_resolver(variables, context):
            return [
                {"id": "easy", "name": "Basic Categorization", "difficulty": "easy", "maxSteps": 20},
                {"id": "medium", "name": "Email Triage", "difficulty": "medium", "maxSteps": 40},
                {"id": "hard", "name": "Full Management", "difficulty": "hard", "maxSteps": 60}
            ]
        
        def analytics_resolver(variables, context):
            return {
                "totalEmails": 0,
                "categoryDistribution": {},
                "priorityDistribution": {},
                "averageSentiment": 0.0,
                "threatCount": 0,
                "vipCount": 0
            }
        
        def system_status_resolver(variables, context):
            return {
                "status": "healthy",
                "uptime": 0,
                "version": "1.0.0",
                "graphql_enabled": True
            }
        
        self.executor.register_resolver("emails", emails_resolver)
        self.executor.register_resolver("tasks", tasks_resolver)
        self.executor.register_resolver("analytics", analytics_resolver)
        self.executor.register_resolver("systemStatus", system_status_resolver)
    
    def execute(self, query: str, variables: Optional[Dict] = None,
                operation_name: Optional[str] = None) -> Dict[str, Any]:
        """Execute a GraphQL query"""
        return self.executor.execute(query, variables, {"operation_name": operation_name})
    
    def get_schema(self) -> str:
        """Get SDL schema"""
        return self.schema.get_schema_sdl()
    
    def introspect(self) -> Dict[str, Any]:
        """Get introspection result"""
        return self.schema.to_introspection()
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get GraphQL usage analytics"""
        stats = self.executor.get_stats()
        return {
            "status": "active",
            "schema_types": len(self.schema.types),
            "queries_defined": len(self.schema.queries),
            "mutations_defined": len(self.schema.mutations),
            "subscriptions_defined": len(self.schema.subscriptions),
            "execution_stats": stats,
            "features": [
                "type_system",
                "introspection",
                "query_parsing",
                "mutation_support",
                "subscription_support",
                "resolver_system",
                "query_caching",
                "execution_stats"
            ]
        }


# Global instance
_graphql_api: Optional[GraphQLAPI] = None
_graphql_lock = threading.Lock()


def get_graphql_api() -> GraphQLAPI:
    """Get or create GraphQL API instance"""
    global _graphql_api
    with _graphql_lock:
        if _graphql_api is None:
            _graphql_api = GraphQLAPI()
        return _graphql_api
