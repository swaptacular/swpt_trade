import os
import shutil
from setuptools import Extension
from setuptools.dist import Distribution
from distutils.command.build_ext import build_ext

from Cython.Build import cythonize

compile_args = []
link_args = []
include_dirs = []
libraries = []

def build():
    extensions = [
        Extension(
            "*",
            ["swpt_trade/*.pyx"],
            extra_compile_args=compile_args,
            extra_link_args=link_args,
            include_dirs=include_dirs,
            libraries=libraries,
        ),
        Extension(
            "*",
            ["tests/*.pyx"],
            extra_compile_args=compile_args,
            extra_link_args=link_args,
            include_dirs=include_dirs,
            libraries=libraries,
        ),
    ]
    ext_modules = cythonize(
        extensions,
        include_path=include_dirs,
        compiler_directives={
            "language_level": 3,
        },
        annotate=False,
    )
    distribution = Distribution({"ext_modules": ext_modules})

    cmd = build_ext(distribution)
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
    build()
