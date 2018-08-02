from mmap import mmap
import os
from pathlib import Path, PathLike
from io import TextIOBase

ENTRY_SIZE = 8  # 8 bytes per entry

class Database:
    db_path: PathLike  # Recomomended to be the path to a sparse file.
    transaction_path: PathLike
    db_fd: int  # File descriptor
    db: mmap
    transaction_file: TextIOBase
    
    def __init__(self, db_path: PathLike, transaction_path: PathLike):
        self.db_path = db_path
        self.transaction_path = transaction_path

    def __enter__(self):
        self.transaction_file = open(self.transaction_path, 'at').__enter__()
        # TODO: Make sure transaction_file is intact;
        # if not, roll forwards or backwards as necessary.
        
        self.db_fd = os.open(self.db_path, os.O_RDWR)
        self.db = mmap(self.db_fd, 0).__enter__()
        # TODO: Check for header. If it's all 0s, write a header.
        # Otherwise, ensure that header is correct
        
        return self

    def __exit__(self, type_, value, traceback):
        self.db.flush()  # Just in case!
        self.db.__exit__(type_, value, traceback)
        os.close(self.db_fd)

        self.transaction_file.__exit__(type_, value, traceback)

    def __getitem__(self, index: int):
        # 0 isn't a valid index; it's where the header's stored.
        if not 0 < index < self.db.size() // ENTRY_SIZE:
            raise ValueError()
        raw = self.db[index * ENTRY_SIZE : (index + 1) * ENTRY_SIZE]
