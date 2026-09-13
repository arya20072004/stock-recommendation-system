with open('tests/test_nse_fallback_historical.py', 'r') as f:
    lines = f.read().splitlines()

lines = [line.rstrip() for line in lines]

while lines and not lines[-1].strip():
    lines.pop()

with open('tests/test_nse_fallback_historical.py', 'w') as f:
    f.write('\n'.join(lines) + '\n')
