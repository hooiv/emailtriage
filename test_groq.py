#!/usr/bin/env python3
"""Quick test of Groq API with your environment"""
import os
from openai import OpenAI

# Set up Groq
os.environ["API_BASE_URL"] = "https://api.groq.com/openai/v1"
os.environ["MODEL_NAME"] = "llama-3.1-70b-versatile"
os.environ["OPENAI_API_KEY"] = "YOUR_GROQ_API_KEY"

client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key="YOUR_GROQ_API_KEY"
)

print("Testing Groq API...")
print("=" * 60)

# Test API call
response = client.chat.completions.create(
    model="llama-3.1-70b-versatile",
    messages=[
        {"role": "system", "content": "You are a helpful email assistant."},
        {"role": "user", "content": "Categorize this email: 'Subject: Urgent customer complaint about product defect'"}
    ],
    temperature=0.2,
    max_tokens=100
)

print(f"✅ Groq API Working!")
print(f"Model: {response.model}")
print(f"Response: {response.choices[0].message.content}")
print("=" * 60)
print("\n🚀 Ready to run full inference!")
