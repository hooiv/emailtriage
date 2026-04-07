"""
Combined FastAPI + Gradio for Email Triage OpenEnv
===================================================
Provides both REST API endpoints AND a visual Gradio UI.
"""

import gradio as gr
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, List
from pydantic import BaseModel

from environment import EmailTriageEnv
from models import Action, Observation, Reward
from tasks import TASKS

# ========== FastAPI Setup ==========
fastapi_app = FastAPI(title="Email Triage OpenEnv", version="1.0.0")

fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global environment for API
api_env: Optional[EmailTriageEnv] = None

class ResetRequest(BaseModel):
    task_id: str = "task_easy_categorize"

class StepRequest(BaseModel):
    action_type: str
    email_id: str = "none"
    category: Optional[str] = None
    priority: Optional[str] = None
    reply_content: Optional[str] = None
    flag_reason: Optional[str] = None

@fastapi_app.get("/health")
def health():
    return {"status": "healthy", "service": "email-triage-openenv"}

@fastapi_app.get("/tasks")
def list_tasks():
    try:
        return {"tasks": [{"id": t.task_id, "name": t.task_name, "difficulty": t.difficulty, "description": t.description} for t in TASKS.values()]}
    except Exception as e:
        return {"error": str(e)}

@fastapi_app.post("/reset")
def reset(request: Optional[ResetRequest] = None):
    global api_env
    try:
        task_id = request.task_id if request else "task_easy_categorize"
        api_env = EmailTriageEnv(task_id=task_id)
        result = api_env.reset()
        return {
            "observation": result.observation.model_dump(),
            "info": result.info,
            "done": False
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}

@fastapi_app.post("/step")
def step(request: StepRequest):
    global api_env
    if api_env is None:
        raise HTTPException(status_code=400, detail="Environment not initialized. Call /reset first.")
    action = Action(**request.model_dump(exclude_none=True))
    result = api_env.step(action)
    return {
        "observation": result.observation.model_dump(),
        "reward": result.reward.model_dump(),
        "done": result.done,
        "info": result.info
    }

@fastapi_app.get("/state")
def state():
    global api_env
    if api_env is None:
        raise HTTPException(status_code=400, detail="Environment not initialized. Call /reset first.")
    return api_env.state()

# ========== Gradio UI ==========
ui_env = None
action_history: List[Dict[str, Any]] = []

def format_email_html(email: dict) -> str:
    priority_colors = {"urgent": "#ef4444", "high": "#f97316", "normal": "#3b82f6", "low": "#6b7280"}
    priority = email.get("priority")
    category = email.get("category")
    is_spam = email.get("is_spam", False)
    is_archived = email.get("is_archived", False)
    
    tags = []
    if is_spam:
        tags.append('<span style="background:#ef4444;color:white;padding:2px 6px;border-radius:4px;font-size:11px;">SPAM</span>')
    if is_archived:
        tags.append('<span style="background:#6b7280;color:white;padding:2px 6px;border-radius:4px;font-size:11px;">ARCHIVED</span>')
    if category:
        tags.append(f'<span style="background:#3b82f6;color:white;padding:2px 6px;border-radius:4px;font-size:11px;">{category}</span>')
    if priority:
        tags.append(f'<span style="background:{priority_colors.get(priority, "#9ca3af")};color:white;padding:2px 6px;border-radius:4px;font-size:11px;">{priority.upper()}</span>')
    
    # Dark-mode friendly: use dark background with light text
    bg_color = "#374151" if is_archived else "#1f2937"
    body_bg = "#111827"
    
    return f"""
    <div style="border:1px solid #4b5563;border-radius:8px;padding:12px;margin:8px 0;background:{bg_color};color:#f3f4f6;">
        <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
            <span style="font-weight:bold;color:#f9fafb;">{email["id"]}</span>
        </div>
        <div style="color:#e5e7eb;"><b>From:</b> {email.get("sender_name", email.get("sender", "Unknown"))}</div>
        <div style="color:#e5e7eb;"><b>Subject:</b> {email.get("subject", "No subject")}</div>
        <div style="margin:8px 0;padding:8px;background:{body_bg};border-radius:4px;font-size:13px;max-height:80px;overflow:hidden;color:#d1d5db;">{str(email.get("body", ""))[:200]}</div>
        <div>{" ".join(tags) if tags else '<span style="color:#9ca3af;">Uncategorized</span>'}</div>
    </div>
    """

def format_inbox_html(observation: dict) -> str:
    inbox = observation.get("inbox", [])
    if not inbox:
        return '<div style="text-align:center;padding:40px;color:#9ca3af;">Inbox empty</div>'
    html = f'<div style="padding:10px;"><h3 style="color:#f3f4f6;">Inbox ({len(inbox)} emails)</h3>'
    for email in inbox[:8]:
        html += format_email_html(email)
    html += '</div>'
    return html

def reset_ui(task_id: str):
    global ui_env, action_history
    action_history = []
    ui_env = EmailTriageEnv(task_id=task_id)
    result = ui_env.reset()
    obs = result.observation.model_dump()
    email_ids = [e["id"] for e in obs.get("inbox", [])]
    return (
        format_inbox_html(obs),
        f"Step: 0 | Task: {task_id}",
        "",
        gr.Dropdown(choices=email_ids, value=email_ids[0] if email_ids else None),
        "Reset complete!"
    )

def take_action_ui(email_id: str, action_type: str, category: str, priority: str, reply_content: str):
    global ui_env, action_history
    if ui_env is None:
        return ("", "", "", gr.Dropdown(), "Reset first!")
    
    action_dict = {"action_type": action_type, "email_id": email_id or "none"}
    if action_type == "categorize" and category:
        action_dict["category"] = category
    if action_type == "prioritize" and priority:
        action_dict["priority"] = priority
    if action_type == "reply" and reply_content:
        action_dict["reply_content"] = reply_content
    
    try:
        action = Action(**action_dict)
        result = ui_env.step(action)
        obs = result.observation.model_dump()
        reward = result.reward.model_dump()
        info = result.info
        
        action_history.append({"action": action_type, "email": email_id, "reward": reward.get("value", 0)})
        
        history_html = "<div style='color:#e5e7eb;'>"
        for h in reversed(action_history[-5:]):
            color = "#22c55e" if h["reward"] > 0 else "#ef4444" if h["reward"] < 0 else "#9ca3af"
            history_html += f'<div style="border-left:3px solid {color};padding:4px;margin:2px 0;background:#1f2937;color:#e5e7eb;">{h["action"]} -> {h["email"]} <span style="color:{color};">{h["reward"]:+.2f}</span></div>'
        history_html += "</div>"
        
        status = f"Reward: {reward.get('value', 0):+.2f} | {reward.get('message', '')}"
        if result.done:
            final_score = info.get("final_score", 0)
            status = f"DONE! Final Score: {final_score:.0%}"
        
        email_ids = [e["id"] for e in obs.get("inbox", []) if not e.get("is_archived") and not e.get("is_spam")]
        metrics = f"Step: {obs.get('step_count', 0)} | Processed: {obs.get('metrics', {}).get('emails_processed', 0)}"
        
        return (
            format_inbox_html(obs),
            metrics,
            history_html,
            gr.Dropdown(choices=email_ids, value=email_ids[0] if email_ids else None),
            status
        )
    except Exception as e:
        return ("", "", "", gr.Dropdown(), f"Error: {e}")

# Build Gradio UI
with gr.Blocks(title="Email Triage OpenEnv") as demo:
    gr.Markdown("# Email Triage OpenEnv\n**OpenEnv-compliant** AI training environment")
    
    with gr.Row():
        with gr.Column(scale=1):
            task_dd = gr.Dropdown(
                choices=[("Easy", "task_easy_categorize"), ("Medium", "task_medium_triage"), ("Hard", "task_hard_full_inbox")],
                value="task_easy_categorize",
                label="Task"
            )
            reset_btn = gr.Button("Reset", variant="primary")
            
            gr.Markdown("### Action")
            email_dd = gr.Dropdown(label="Email", choices=[])
            action_dd = gr.Dropdown(
                choices=["categorize", "prioritize", "reply", "archive", "flag", "mark_spam", "done"],
                value="categorize",
                label="Type"
            )
            cat_dd = gr.Dropdown(choices=["customer_support", "sales", "billing", "technical", "spam", "internal"], label="Category")
            pri_dd = gr.Dropdown(choices=["urgent", "high", "normal", "low"], label="Priority")
            reply_txt = gr.Textbox(label="Reply", lines=2)
            action_btn = gr.Button("Execute")
            
            status_txt = gr.Markdown("*Click Reset*")
            metrics_txt = gr.Markdown("")
            history_html = gr.HTML()
        
        with gr.Column(scale=2):
            inbox_html = gr.HTML('<div style="text-align:center;padding:40px;color:#9ca3af;">Click Reset to start</div>')
    
    reset_btn.click(reset_ui, [task_dd], [inbox_html, metrics_txt, history_html, email_dd, status_txt])
    action_btn.click(take_action_ui, [email_dd, action_dd, cat_dd, pri_dd, reply_txt], [inbox_html, metrics_txt, history_html, email_dd, status_txt])
    
    gr.Markdown("---\n**API:** `/health`, `/tasks`, `/reset`, `/step`, `/state`")

# Mount Gradio at root for HF Spaces App tab compatibility
app = gr.mount_gradio_app(fastapi_app, demo, path="/")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7860)
