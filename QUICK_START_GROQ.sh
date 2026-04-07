#!/bin/bash
# Quick Start Script for Email Triage OpenEnv with Groq

echo "═══════════════════════════════════════════════════════════"
echo "  Email Triage OpenEnv - Quick Start with Groq (FREE)"
echo "═══════════════════════════════════════════════════════════"
echo ""

# Set Groq credentials
export API_BASE_URL="https://api.groq.com/openai/v1"
export MODEL_NAME="llama-3.3-70b-versatile"
export OPENAI_API_KEY="YOUR_GROQ_API_KEY"

# Option 1: Use HF Space (default)
export ENV_URL="https://ervjn455-email-triage-openenv.hf.space"

# Option 2: Use local server (uncomment if HF Space is down)
# export ENV_URL="http://localhost:7860"

echo "Configuration:"
echo "   API Provider: Groq (FREE)"
echo "   Model: llama-3.3-70b-versatile"
echo "   Environment: $ENV_URL"
echo ""
echo "Running inference on all 3 tasks..."
echo ""

python3 inference.py

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Inference complete! Check inference_results.json"
echo "═══════════════════════════════════════════════════════════"
