import platform
print("Welcome to Python")
print("Just making a Change to check commit")
print(platform.platform())
print(platform.platform(0))
print(platform.platform(0))
print(platform.platform(0,0))
print(platform.platform(0,1))
print(platform.platform(1,0))
print(platform.platform(1,1))

print(platform.machine())
print(platform.processor())
print(platform.system())
print(platform.version())

print(platform.python_implementation())

for atr in platform.python_version_tuple():
    print(atr)
