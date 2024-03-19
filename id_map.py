

self = {
    "id_map": {}, 
    "max_id": 0
}

def create(k):
    self["max_id"] += 1
    self["id_map"][k] = self["max_id"]
    return self["max_id"]
    

def get(k):
    return self["id_map"][k]

def dump():
    print(self["id_map"])
