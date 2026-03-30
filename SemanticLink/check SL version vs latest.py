#!/usr/bin/env python
# coding: utf-8

# ## check SL version vs latest
# 
# null

# In[1]:


import subprocess
import sys
import json
import urllib.request
from packaging.version import Version
from importlib.metadata import version, PackageNotFoundError

def get_latest_version(package_name):
    """Fetch latest version from PyPI"""
    try:
        url = f"https://pypi.org/pypi/{package_name}/json"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read())
            return data['info']['version']
    except Exception as e:
        print(f"⚠ Could not fetch latest version: {e}")
        return None

# Check semantic-link version (pre-installed, don't reinstall)
try:
    installed_version = version('semantic-link-sempy')
    latest_version = get_latest_version('semantic-link')
    print(f"Current semantic-link version: {installed_version}")
    if latest_version:
        print(f"Latest semantic-link version: {latest_version}")
        if Version(installed_version) < Version(latest_version):
            print(f"⚠ Warning: semantic-link version {installed_version} < {latest_version}")
    else:
        print("⚠ Could not determine latest version")
except PackageNotFoundError:
    print("⚠ semantic-link not found (unexpected in Fabric)")

# Check semantic-link-labs version
try:
    labs_version = version('semantic-link-labs')
    print(f"Current semantic-link-labs version: {labs_version}")
except PackageNotFoundError:
    labs_version = None
    print("semantic-link-labs not found")

# Install/upgrade semantic-link-labs only
print("\n--- Installing/upgrading semantic-link-labs ---")
result = subprocess.run(
    [sys.executable, "-m", "pip", "install", "--upgrade", "--quiet", "semantic-link-labs"],
    capture_output=True,
    text=True
)
if result.returncode == 0:
    print("✓ semantic-link-labs installed/upgraded successfully")
else:
    print(f"⚠ Installation warning: {result.stderr[:200]}")

