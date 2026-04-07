#!/usr/bin/env python3
"""Test the deployed HF Space to ensure it's competition-ready"""
import httpx
import json

BASE_URL = "https://ervjn455-email-triage-openenv.hf.space"

print("=" * 60)
print("HACKATHON DEPLOYMENT VERIFICATION")
print("=" * 60)

# Test 1: Health check
print("\n✓ Test 1: Health Check")
response = httpx.get(f"{BASE_URL}/health", timeout=10)
assert response.status_code == 200, f"Health check failed: {response.status_code}"
print(f"  Status: {response.json()['status']}")

# Test 2: List tasks
print("\n✓ Test 2: List Tasks")
response = httpx.get(f"{BASE_URL}/tasks", timeout=10)
tasks = response.json()["tasks"]
assert len(tasks) >= 3, f"Expected 3+ tasks, got {len(tasks)}"
print(f"  Tasks available: {len(tasks)}")
for task in tasks:
    print(f"    - {task['task_id']} ({task['difficulty']})")

# Test 3: Reset environment
print("\n✓ Test 3: Reset Environment")
response = httpx.post(f"{BASE_URL}/reset", json={"task_id": "task_easy_categorize"}, timeout=30)
assert response.status_code == 200, f"Reset failed: {response.status_code}"
result = response.json()
assert "observation" in result, "Missing observation in reset response"
assert "info" in result, "Missing info in reset response"
print(f"  Emails in inbox: {len(result['observation']['inbox'])}")
print(f"  Max steps: {result['info']['max_steps']}")

# Test 4: Take action (step)
print("\n✓ Test 4: Step (Take Action)")
action = {
    "action_type": "categorize",
    "email_id": result['observation']['inbox'][0]['id'],
    "category": "internal"
}
response = httpx.post(f"{BASE_URL}/step", json=action, timeout=30)
assert response.status_code == 200, f"Step failed: {response.status_code}"
result = response.json()
assert "reward" in result, "Missing reward in step response"
assert "observation" in result, "Missing observation in step response"
assert "done" in result, "Missing done in step response"
print(f"  Reward: {result['reward']['value']}")
print(f"  Done: {result['done']}")
print(f"  Message: {result['reward']['message']}")

# Test 5: State endpoint
print("\n✓ Test 5: Get State")
response = httpx.get(f"{BASE_URL}/state", timeout=10)
assert response.status_code == 200, f"State failed: {response.status_code}"
state = response.json()
print(f"  Current step: {state['step_count']}")

print("\n" + "=" * 60)
print("✅ ALL TESTS PASSED - DEPLOYMENT IS COMPETITION-READY!")
print("=" * 60)
print(f"\nSpace URL: {BASE_URL}")
print("API Docs: {}/docs".format(BASE_URL))
