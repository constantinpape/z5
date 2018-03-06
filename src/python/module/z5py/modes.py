import os
import errno
import shutil

DEFAULT = 'a'

MODES = {'r', 'r+', 'w', 'w-', 'x', 'a'}

MUST_EXIST = {'r', 'r+'}
MUST_NOT_EXIST = {'w-', 'x'}
CAN_WRITE = {'w', 'w-', 'x', 'a', 'r+'}
SHOULD_TRUNCATE = {'w'}


class FileMode(object):
    def __init__(self, mode='a'):
        mode_as_str = str(mode)
        if mode_as_str not in MODES:
            raise ValueError('Mode {} does not exist: must be one of ({})'.format(mode, ', '.join(sorted(MODES))))
        self.value = mode_as_str

        self.file_must_exist = mode_as_str in MUST_EXIST
        self.file_must_not_exist = mode_as_str in MUST_NOT_EXIST
        self.can_write = mode_as_str in CAN_WRITE
        self.should_truncate_file = mode_as_str in SHOULD_TRUNCATE

    def check_file(self, path):
        """Raise errors as appropriate given file permissions, truncate if necessary, then return whether path exists"""
        if os.path.exists(path):
            if self.file_must_not_exist:
                raise OSError(errno.EEXIST, os.strerror(errno.EEXIST), path)
            if self.should_truncate_file:
                shutil.rmtree(path)
            else:
                return True

        if not os.path.exists(path):
            if self.file_must_exist:
                raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)
            return False

    def check_write(self, path):
        """Raise error as appropriate given file permissions, then return whether path exists"""
        if not self.can_write:
            raise OSError(errno.EROFS, os.strerror(errno.EROFS), path)
        return os.path.exists(path)

    def __str__(self):
        return self.value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return hash(self) == hash(other)
