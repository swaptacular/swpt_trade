#! /bin/env python
import os
import os.path
import shutil
from setuptools import Extension
from setuptools.dist import Distribution
from distutils.command.build_ext import build_ext
from Cython.Build import cythonize

COMPILE_ARGS = []
LINK_ARGS = []
INCLUDE_DIRS = []
LIBRARIES = []


def build():
    kwargs = {
        "extra_compile_args": COMPILE_ARGS,
        "extra_link_args": LINK_ARGS,
        "include_dirs": INCLUDE_DIRS,
        "libraries": LIBRARIES,
    }
    extensions = [
        Extension("*", ["swpt_trade/*.pyx"], **kwargs),
        Extension("*", ["tests/*.pyx"], **kwargs),
    ]
    ext_modules = cythonize(
        extensions,
        include_path=INCLUDE_DIRS,
        compiler_directives={
            "language_level": 3,
        },
    )
    cmd = build_ext(Distribution({"ext_modules": ext_modules}))
    cmd.ensure_finalized()
    cmd.run()

    # Copy built extensions back to the project
    for output in cmd.get_outputs():
        relative_extension = os.path.relpath(output, cmd.build_lib)
        shutil.copyfile(output, relative_extension)
        mode = os.stat(relative_extension).st_mode
        mode |= (mode & 0o444) >> 2
        os.chmod(relative_extension, mode)


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    build()
