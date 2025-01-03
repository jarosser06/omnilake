import fnmatch

from pathlib import Path
from typing import List, Union


def collect_files(directory: Union[str, Path], ignore_patterns: List[str] = None) -> List[Path]:
    """
    Collect all files in a directory, respecting ignore patterns.
    
    Args:
    directory -- the directory to collect files from
    ignore_patterns -- a list of patterns to ignore (gitignore-style)
    """
    if ignore_patterns is None:
        ignore_patterns = []
    
    pth = Path(directory)

    files = []
    
    for file in pth.rglob('*'):

        if file.is_file():
            relative_path = file.relative_to(pth)

            if not any(fnmatch.fnmatch(str(relative_path), pattern) for pattern in ignore_patterns):
                files.append(file)
    
    return files