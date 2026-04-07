#!/usr/bin/env python3
"""
Email Triage OpenEnv - Baseline Inference Script
=================================================

MANDATORY STDOUT FORMAT:
    [START] task=<task_name> env=<benchmark> model=<model_name>
    [STEP]  step=<n> action=<action_str> reward=<0.00> done=<true|false> error=<msg|null>
    [END]   success=<true|false> steps=<n> score=<score> rewards=<r1,r2,...,rn>

Required environment variables:
- API_BASE_URL: The API endpoint for the LLM
- MODEL_NAME: The model identifier to use for inference
- HF_TOKEN: API key for authentication
"""

import os
import sys
import json
import re
from typing import Any, Dict, List, Optional

import httpx
from openai import OpenAI


# Configuration from environment
API_BASE_URL = os.environ.get("API_BASE_URL") or "https://router.huggingface.co/v1"
MODEL_NAME = os.environ.get("MODEL_NAME") or "Qwen/Qwen2.5-72B-Instruct"
API_KEY = os.environ.get("HF_TOKEN") or os.environ.get("API_KEY") or os.environ.get("OPENAI_API_KEY") or ""

# Environment URL - default to HF Space URL
ENV_URL = os.environ.get("ENV_URL", "https://ervjn455-email-triage-openenv.hf.space")
BENCHMARK = "email-triage-openenv"

# Agent settings
MAX_TOKENS = 1024
TEMPERATURE = 0.2

# Task configurations
TASKS = [
    {"task_id": "task_easy_categorize", "name": "easy_categorize"},
    {"task_id": "task_medium_triage", "name": "medium_triage"},
    {"task_id": "task_hard_full_inbox", "name": "hard_full_inbox"},
]


# ============== MANDATORY LOGGING FUNCTIONS ==============

def log_start(task: str, env: str, model: str) -> None:
    """Log episode start in required format."""
    print(f"[START] task={task} env={env} model={model}", flush=True)


def log_step(step: int, action: str, reward: float, done: bool, error: Optional[str]) -> None:
    """Log step in required format."""
    error_val = error if error else "null"
    done_val = str(done).lower()
    # Escape action string for clean output
    action_clean = action.replace('\n', ' ').replace('\r', '')[:100]
    print(
        f"[STEP] step={step} action={action_clean} reward={reward:.2f} done={done_val} error={error_val}",
        flush=True,
    )


def log_end(success: bool, steps: int, score: float, rewards: List[float]) -> None:
    """Log episode end in required format."""
    rewards_str = ",".join(f"{r:.2f}" for r in rewards)
    print(f"[END] success={str(success).lower()} steps={steps} score={score:.2f} rewards={rewards_str}", flush=True)


SYSTEM_PROMPT = """You are an expert AI email assistant helping to efficiently triage and manage an email inbox.

Your goal is to process each email by:
1. Reading the email content carefully
2. Categorizing it into the correct category:
   - customer_support: Customer complaints, order issues, account problems, refund requests
   - sales: Enterprise inquiries, demo requests, partnership opportunities, pricing questions
   - billing: Payment issues, invoice questions, subscription changes
   - technical: Bug reports, API issues, feature requests, security reports
   - spam: Obvious scams, phishing attempts, lottery wins, suspicious offers
   - internal: Team meetings, internal communications, PTO requests, company updates
   - newsletter: Digests, product updates, marketing emails, subscriptions
3. Setting appropriate priority:
   - urgent: Production down, security issues, angry customers threatening action
   - high: Important customer issues, large sales opportunities, time-sensitive billing
   - normal: Standard requests, routine communications
   - low: Newsletters, FYI emails, low-priority feature requests
4. Taking appropriate actions based on category and priority

CRITICAL DECISION RULES:
- SPAM: Mark as spam immediately. Look for: lottery wins, phishing links, suspicious URLs, ALL CAPS excitement, requests for personal info, ".xyz" domains
- CUSTOMER_SUPPORT with HIGH/URGENT priority: Reply with acknowledgment, then flag
- TECHNICAL with "CRITICAL" or "P1": Forward to tech-support@company.com, flag
- NEWSLETTER: Archive after categorizing
- SALES inquiries: Prioritize based on deal size hints (enterprise = high, small = normal)

SENDER TRUST SCORING (use when deciding priority):
- VIP senders (trust_score >= 0.9): Prioritize higher
- Suspicious senders (trust_score < 0.3): Be cautious, likely spam
- Unknown senders: Use content to judge

SMART SUGGESTIONS:
The system provides suggested_category and confidence_score for each email. Use these as hints:
- confidence >= 0.8: Strong suggestion, likely correct
- confidence 0.5-0.8: Moderate confidence, verify with content
- confidence < 0.5: Low confidence, rely on your judgment

SLA TRACKING:
Some emails have sla_deadline and sla_priority. Prioritize emails with:
- sla_priority == "critical": Handle immediately
- time_in_inbox_hours > 4: Getting stale, prioritize

Available actions (use one at a time):
- categorize: {"action_type": "categorize", "email_id": "<id>", "category": "<category>"}
- prioritize: {"action_type": "prioritize", "email_id": "<id>", "priority": "<priority>"}
- reply: {"action_type": "reply", "email_id": "<id>", "reply_content": "<professional response>"}
- forward: {"action_type": "forward", "email_id": "<id>", "forward_to": "<email>"}
- archive: {"action_type": "archive", "email_id": "<id>"}
- flag: {"action_type": "flag", "email_id": "<id>"}
- mark_spam: {"action_type": "mark_spam", "email_id": "<id>"}
- snooze: {"action_type": "snooze", "email_id": "<id>", "snooze_hours": <hours>}
- done: {"action_type": "done"}

BATCH ACTIONS (for efficiency):
You can process multiple emails at once:
{"action_type": "batch", "email_id": "batch", "batch_actions": [
  {"email_id": "<id1>", "action_type": "archive"},
  {"email_id": "<id2>", "action_type": "mark_spam"}
]}

WORKFLOW per email:
1. Categorize first (use suggested_category if confidence > 0.7)
2. Set priority
3. Take action (reply/forward/archive/flag/mark_spam based on rules)
4. Move to next email

Respond with ONLY a valid JSON action object. Be efficient - you have limited steps."""


def build_user_prompt(
    step: int,
    observation: Dict[str, Any],
    history: List[str]
) -> str:
    """Build the user prompt from observation."""
    inbox = observation.get("inbox", [])
    task_desc = observation.get("task_description", "")
    step_count = observation.get("step_count", 0)
    max_steps = observation.get("max_steps", 20)
    last_result = observation.get("last_action_result", "")
    last_error = observation.get("last_action_error", "")
    recommended_actions = observation.get("recommended_actions", [])
    
    prompt_parts = [
        f"TASK: {task_desc}",
        f"Progress: Step {step_count}/{max_steps} ({max_steps - step_count} remaining)",
        ""
    ]
    
    if last_result:
        prompt_parts.append(f"✓ Last action: {last_result}")
    if last_error:
        prompt_parts.append(f"✗ Error: {last_error}")
    
    # Show high-confidence recommendations from the system
    if recommended_actions:
        high_conf = [r for r in recommended_actions if r.get("confidence", 0) >= 0.8]
        if high_conf:
            prompt_parts.append("\n🎯 HIGH-CONFIDENCE RECOMMENDATIONS:")
            for rec in high_conf[:3]:
                prompt_parts.append(
                    f"  - {rec['email_id']}: {rec['suggested_action']} "
                    f"({rec.get('suggested_category', '')}) "
                    f"[{rec['confidence']:.0%}] - {rec.get('reason', '')}"
                )
    
    # Separate processed and unprocessed emails
    unprocessed = [e for e in inbox if not e.get("category")]
    processed = [e for e in inbox if e.get("category")]
    
    prompt_parts.append(f"\n📬 INBOX STATUS: {len(processed)}/{len(inbox)} emails processed")
    
    if unprocessed:
        prompt_parts.append("\n=== UNPROCESSED EMAILS (prioritize these) ===")
        for email in unprocessed[:5]:  # Show top 5 unprocessed
            prompt_parts.append(f"\n📧 ID: {email['id']}")
            prompt_parts.append(f"From: {email['sender_name']} <{email['sender']}>")
            prompt_parts.append(f"Subject: {email['subject']}")
            
            # Add sender trust info
            sender_info = email.get("sender_info", {})
            if sender_info:
                trust = sender_info.get("trust_score", 0.5)
                sender_type = sender_info.get("sender_type", "unknown")
                if trust >= 0.9:
                    prompt_parts.append(f"👤 Sender: VIP (trust: {trust:.0%})")
                elif trust < 0.3:
                    prompt_parts.append(f"⚠️ Sender: SUSPICIOUS (trust: {trust:.0%})")
                else:
                    prompt_parts.append(f"👤 Sender: {sender_type} (trust: {trust:.0%})")
            
            # Add suggested category with confidence
            suggested_cat = email.get("suggested_category")
            confidence = email.get("confidence_score", 0)
            if suggested_cat and confidence > 0:
                prompt_parts.append(f"💡 Suggestion: {suggested_cat} (confidence: {confidence:.0%})")
            
            # Add SLA info if present
            sla_priority = email.get("sla_priority")
            time_in_inbox = email.get("time_in_inbox_hours", 0)
            if sla_priority:
                prompt_parts.append(f"⏰ SLA: {sla_priority} priority, in inbox {time_in_inbox:.1f}h")
            elif time_in_inbox > 4:
                prompt_parts.append(f"⏰ In inbox: {time_in_inbox:.1f}h (getting stale)")
            
            body_preview = email['body'][:350].replace('\n', ' ')
            prompt_parts.append(f"Preview: {body_preview}...")
    
    if processed:
        prompt_parts.append("\n=== PROCESSED EMAILS ===")
        for email in processed:
            flags = []
            flags.append(f"Cat:{email.get('category', '?')}")
            if email.get("priority"):
                flags.append(f"Pri:{email['priority']}")
            if email.get("is_flagged"):
                flags.append("🚩")
            if email.get("is_spam"):
                flags.append("🚫SPAM")
            if email.get("is_archived"):
                flags.append("📦")
            if email.get("reply_sent"):
                flags.append("↩️REPLIED")
            if email.get("forwarded_to"):
                flags.append(f"→{email['forwarded_to']}")
            
            prompt_parts.append(f"  {email['id']}: [{' | '.join(flags)}]")
    
    # Remind about pending actions
    needs_action = []
    for email in inbox:
        cat = email.get("category")
        if cat == "customer_support" and not email.get("reply_sent"):
            needs_action.append(f"{email['id']}: needs reply (customer_support)")
        if cat == "technical" and not email.get("forwarded_to"):
            needs_action.append(f"{email['id']}: needs forward (technical)")
        if cat == "newsletter" and not email.get("is_archived"):
            needs_action.append(f"{email['id']}: should archive (newsletter)")
        if cat == "spam" and not email.get("is_spam"):
            needs_action.append(f"{email['id']}: mark as spam")
    
    if needs_action:
        prompt_parts.append("\n⚠️ PENDING ACTIONS:")
        for action in needs_action[:5]:
            prompt_parts.append(f"  - {action}")
    
    if not unprocessed and not needs_action:
        prompt_parts.append("\n✅ All emails processed! Use 'done' action to complete.")
    
    prompt_parts.append("\nWhat is your next action? Respond with JSON only.")
    
    return "\n".join(prompt_parts)


def parse_model_action(response_text: str) -> Dict[str, Any]:
    """Parse the model's response to extract the action."""
    # Try to extract JSON from response
    json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass
    
    # Try to parse as plain JSON
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        pass
    
    # Try to find JSON object in response
    json_match = re.search(r'\{[^{}]*\}', response_text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group(0))
        except json.JSONDecodeError:
            pass
    
    # Fallback: return done action
    return {"action_type": "done"}


def run_episode(
    client: OpenAI,
    http_client: httpx.Client,
    task_id: str,
    task_name: str
) -> Dict[str, Any]:
    """Run a single episode on a task with mandatory logging format."""
    
    rewards: List[float] = []
    steps_taken = 0
    score = 0.0
    success = False
    last_error: Optional[str] = None
    
    # Log start
    log_start(task=task_name, env=BENCHMARK, model=MODEL_NAME)
    
    try:
        # Reset environment
        reset_response = http_client.post(
            f"{ENV_URL}/reset",
            json={"task_id": task_id}
        )
        reset_response.raise_for_status()
        reset_data = reset_response.json()
        
        observation = reset_data["observation"]
        info = reset_data.get("info", {})
        max_steps = info.get("max_steps", 20)
        
        history: List[str] = []
        done = False
        
        while not done and steps_taken < max_steps:
            steps_taken += 1
            
            # Build prompt
            user_prompt = build_user_prompt(steps_taken, observation, history)
            
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt}
            ]
            
            # Get model response
            try:
                completion = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=messages,
                    temperature=TEMPERATURE,
                    max_tokens=MAX_TOKENS
                )
                response_text = completion.choices[0].message.content or ""
            except Exception as e:
                response_text = '{"action_type": "done"}'
                last_error = str(e)
            
            # Parse action
            action = parse_model_action(response_text)
            action_str = f"{action.get('action_type', 'unknown')}({action.get('email_id', '')})"
            
            # Execute action
            try:
                step_response = http_client.post(
                    f"{ENV_URL}/step",
                    json=action
                )
                step_response.raise_for_status()
                step_data = step_response.json()
                
                observation = step_data["observation"]
                reward = step_data["reward"]["value"]
                done = step_data["done"]
                step_error = step_data.get("observation", {}).get("last_action_error")
                
                rewards.append(reward)
                
                # Log step
                log_step(
                    step=steps_taken,
                    action=action_str,
                    reward=reward,
                    done=done,
                    error=step_error
                )
                
                history.append(f"Step {steps_taken}: {action.get('action_type')} -> reward {reward:+.3f}")
                
                if done:
                    score = step_data.get("info", {}).get("final_score", 0.0)
                    success = score >= 0.5  # Success threshold
                    
            except Exception as e:
                last_error = str(e)
                rewards.append(0.0)
                log_step(
                    step=steps_taken,
                    action=action_str,
                    reward=0.0,
                    done=False,
                    error=str(e)
                )
        
        # If not done, force completion
        if not done:
            try:
                final_response = http_client.post(
                    f"{ENV_URL}/step",
                    json={"action_type": "done"}
                )
                final_data = final_response.json()
                score = final_data.get("info", {}).get("final_score", 0.0)
                success = score >= 0.5
            except Exception:
                pass
                
    except Exception as e:
        last_error = str(e)
        
    finally:
        # Always log end
        log_end(success=success, steps=steps_taken, score=score, rewards=rewards)
    
    return {
        "task_id": task_id,
        "task_name": task_name,
        "final_score": score,
        "steps_used": steps_taken,
        "rewards": rewards,
        "success": success
    }


def main() -> None:
    """Main function to run inference on all tasks."""
    # Validate configuration
    if not API_KEY:
        print("[ERROR] HF_TOKEN or API_KEY environment variable is required", flush=True)
        sys.exit(1)
    
    # Initialize clients
    client = OpenAI(base_url=API_BASE_URL, api_key=API_KEY)
    http_client = httpx.Client(timeout=60.0)
    
    # Run on all tasks
    results: List[Dict[str, Any]] = []
    
    for task in TASKS:
        task_id = task["task_id"]
        task_name = task["name"]
        
        try:
            result = run_episode(client, http_client, task_id, task_name)
            results.append(result)
        except Exception as e:
            # Log failed task
            log_start(task=task_name, env=BENCHMARK, model=MODEL_NAME)
            log_end(success=False, steps=0, score=0.0, rewards=[])
            results.append({
                "task_id": task_id,
                "task_name": task_name,
                "final_score": 0.0,
                "error": str(e)
            })
    
    # Write results to file (for local reference)
    import time
    with open("inference_results.json", "w") as f:
        json.dump({
            "results": results,
            "average_score": sum(r.get("final_score", 0) for r in results) / len(results) if results else 0,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }, f, indent=2)


if __name__ == "__main__":
    main()
