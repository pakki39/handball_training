import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

VIDEO_ROOT = os.path.expanduser("~/pve/Handball")
TAG_SCAN_ROOT = os.path.expanduser("~/pve/Handball")
TARGET_ROOT = os.environ.get("TARGET_ROOT", os.path.join(BASE_DIR, "target"))
EXPORT_ROOT = os.environ.get("EXPORT_ROOT", os.path.join(BASE_DIR, "export"))

DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "storage", "app.db"))

ALLOWED_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".avi"}
