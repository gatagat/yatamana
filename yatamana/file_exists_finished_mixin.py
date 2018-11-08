from __future__ import (
        division, print_function, unicode_literals, absolute_import)

import os


class FileExistsFinishedMixin(object):
    """Mixin checking for the existance of the output file.
    """

    def is_finished(self):
        """Return True if the output file exists.

        The output file must be specified by the `self.out_filename` attribute.
        """
        return os.path.exists(self.out_filename)
