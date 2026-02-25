import logging
import requests
import json

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

OLLAMA_API_URL = "http://localhost:11434/api/generate"

def call_ollama_classify(prompt, model="gpt-oss:20b"):
    url = OLLAMA_API_URL
    payload = {"model": model, "prompt": prompt, "stream": True}
    logger.info("Calling Ollama model=%s with prompt length=%d chars", model, len(prompt))
    resp = requests.post(url, json=payload, stream=True, timeout=120)
    resp.raise_for_status()
    full_response = ""
    for line in resp.iter_lines():
        if line:
            try:
                chunk = json.loads(line)
                if "response" in chunk:
                    full_response += chunk["response"]
                if chunk.get("done", False):
                    logger.debug("Ollama stream finished")
                    break
            except json.JSONDecodeError:
                logger.warning("Failed to decode chunk: %s", line)
    logger.debug("Ollama response length=%d chars", len(full_response))
    return full_response.strip()