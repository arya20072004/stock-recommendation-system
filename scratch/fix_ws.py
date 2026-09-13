with open('src/features/v1/engineering.py', 'r') as f:
    lines = f.read().splitlines()

# Remove trailing whitespace from all lines
lines = [line.rstrip() for line in lines]

# Remove blank lines at EOF
while lines and not lines[-1].strip():
    lines.pop()

with open('src/features/v1/engineering.py', 'w') as f:
    f.write('\n'.join(lines) + '\n')
