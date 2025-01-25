
A1 = {'a': 1, 'b':2, 'c':3}
B1 = {'d':4, 'e':5}
C1 = {'f':6}
BASE = {'A': A1, 'B': B1, 'C': C1}
print(f"\nBASE: {BASE}")

A2 = {'aa': 1, 'bb':2, 'cc':3}
B2 = {'dd':4, 'ee':5}
C2 = {'ff':6}
HASH = {'A': A2, 'B': B2, 'C' : C2}
print(f"\nHASH: {HASH}")

print(f"\n\nCOMBINED")
print(BASE | HASH)
COMBINED = {}
for key in BASE:
    COMBINED[key] = BASE[key]
for key in HASH:
    COMBINED[key] = COMBINED[key] | HASH[key]
print(f"\n\nCOMBINED")
print(COMBINED)
