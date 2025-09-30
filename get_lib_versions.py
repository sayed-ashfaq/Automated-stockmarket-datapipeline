import importlib.metadata
from pathlib import Path

file_path = Path.cwd()/ "requirements.txt"

def get_lib_versions(file_path: str) -> list[str]:
    with open(file_path, 'r') as f:
        requirements = f.readlines()
        requirements= [req.split('==')[0] if '==' in req else req.replace('\n','') for req in requirements]
    return requirements

packages = get_lib_versions(file_path)

for package in packages:
    try:
        version = importlib.metadata.version(package)
        print(f"{package}=={version}")
    except importlib.metadata.PackageNotFoundError:
        print(f"{package} is not installed.")