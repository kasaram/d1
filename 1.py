import subprocess
import sys

# Function to install required packages
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# List of required packages
required_packages = [
    "requests",
    "pandas",
    "ta",
    "matplotlib",
    "tabulate"
]

# Install packages if not found
for package in required_packages:
    try:
        __import__(package)
    except ImportError:
        print(f"{package} not found. Installing...")
        install(package)
