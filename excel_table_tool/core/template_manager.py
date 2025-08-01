import os
import json
import shutil

TEMPLATE_DIR = "templates"
os.makedirs(TEMPLATE_DIR, exist_ok=True)

def get_template_path(name):
    return os.path.join(TEMPLATE_DIR, f"{name}.json")

def list_templates():
    return [
        f[:-5] for f in os.listdir(TEMPLATE_DIR)
        if f.endswith(".json")
    ]

def save_template(name, config: dict):
    path = get_template_path(name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

def load_template(name):
    path = get_template_path(name)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def delete_template(name):
    path = get_template_path(name)
    if os.path.exists(path):
        os.remove(path)

def rename_template(old_name, new_name):
    old_path = get_template_path(old_name)
    new_path = get_template_path(new_name)
    if os.path.exists(old_path):
        os.rename(old_path, new_path)

def duplicate_template(src_name, new_name):
    src_path = get_template_path(src_name)
    dst_path = get_template_path(new_name)
    if os.path.exists(src_path):
        shutil.copy(src_path, dst_path)
