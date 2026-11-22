"""檢查目前目錄中指定版本的 GitHub Release 說明，成功時輸出正文。"""

import re
import sys
from pathlib import Path


SECTIONS = ("新增功能", "優化", "Bug 修正", "升級影響", "驗證結果")


def read_notes(tag):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise ValueError(f"版本標籤格式錯誤：{tag}")
    path = Path("docs/releases") / f"{tag}.md"
    notes = path.read_text(encoding="utf-8").strip()
    # HTML 註解可作為作者提示，但不能算作讀者看得到的內容。
    visible = re.sub(r"<!--.*?(?:-->|\Z)", "", notes, flags=re.S).strip()
    lines = visible.splitlines()
    if not lines or not re.fullmatch(rf"# {re.escape(tag)}(?:\s+.+)?", lines[0]):
        raise ValueError(f"{path} 的第一行應為「# {tag}」或在版本後加上標題")
    if re.search(r"\b(?:TODO|TBD|FIXME)\b|待補(?:充)?|待填寫", visible, re.I):
        raise ValueError(f"{path} 含有未完成的占位文字")

    sections = {}
    current = None
    fence = ""
    for line in lines[1:]:
        marker = re.match(r"^ {0,3}(`{3,}|~{3,})", line)
        if fence:
            if line.strip().startswith(fence) and not line.strip().strip(fence[0]):
                fence = ""
            continue
        if marker:
            fence = marker[1]
            continue
        if line.startswith("## "):
            current = line[3:].strip()
            if current in sections:
                raise ValueError(f"{path} 有重複章節：{current}")
            sections[current] = []
        elif current and line.strip():
            sections[current].append(line.strip())
    for title in SECTIONS:
        items = [line[1:].strip() for line in sections.get(title, []) if line == "-" or line.startswith("- ")]
        if not items or any(not re.search(r"\w", item) for item in items):
            raise ValueError(f"{path} 的「{title}」需要至少一項有內容的 Markdown 清單項目")
        if title in ("升級影響", "驗證結果") and all(item.rstrip("。.") in ("無", "本版無此項", "不適用") for item in items):
            raise ValueError(f"{path} 的「{title}」需要具體說明")
    return notes


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("用法：python3 .github/scripts/check_release_notes.py vX.Y.Z")
    try:
        print(read_notes(sys.argv[1]))
    except (OSError, ValueError) as error:
        sys.exit(str(error))
