import subprocess
import hashlib

def normalize_and_hash(content: bytes) -> str:
    content_str = content.decode('utf-8', errors='ignore')
    normalized_lines = [line.rstrip() for line in content_str.splitlines()]
    normalized_content = '\n'.join(normalized_lines).strip() + '\n'
    return hashlib.sha256(normalized_content.encode('utf-8')).hexdigest()

out = subprocess.check_output(['git', 'fsck', '--lost-found'], stderr=subprocess.STDOUT).decode('utf-8')
for line in out.splitlines():
    if line.startswith('dangling blob'):
        blob_id = line.split()[2]
        blob_content = subprocess.check_output(['git', 'cat-file', '-p', blob_id])
        h = normalize_and_hash(blob_content)
        if h == "426253a3d8a9dc6a8d6e4210d825d926c393e717f2f334df4a3de1267912328d":
            print(f"FOUND! Blob {blob_id} has the expected pipeline hash.")
            with open('src/features/v1/engineering.py', 'wb') as f:
                f.write(blob_content)
            print("Successfully restored src/features/v1/engineering.py")
            break
else:
    print("Not found in dangling blobs.")
