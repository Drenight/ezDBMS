filename = 'example.db'
offset = 128  # example offset value

with open(filename, 'r+b') as f:
    f.seek(0)
    f.write(b'xx')
    f.close()

with open(filename, 'r+b') as f:
    f.seek(offset)
    f.write(b'???')
    f.close()

with open(filename, 'r+b') as f:
    f.seek(32)
    f.write(b'123')
    f.close()

with open(filename, 'r+b') as f:
    print(f.read())
    f.seek(16)
    print(f.read(8))
    f.close()