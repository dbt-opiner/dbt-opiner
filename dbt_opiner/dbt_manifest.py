import json
class DbtManifest:
    def __init__(self, manifest_path: str)->None:
        self.manifest_path = manifest_path
        with open(self.manifest_path, 'r') as f:
            self.manifest_dict = json.load(f)
            f.close()
