import importlib
import sys
import os
import os.path

test_names = set()

for dir_entry in os.scandir(os.path.dirname(__file__)):
    if not (
        dir_entry.is_file
        and dir_entry.name.startswith("test_")
        and dir_entry.name.endswith(".pyx")
    ):
        continue

    mod = dir_entry.name[:-4]

    try:
        m = importlib.import_module(f"tests.{mod}")
    except ModuleNotFoundError:
        raise RuntimeError(
            f"{mod}.pyx can not be loaded."
            " All .pyx files must be compiled before running the tests."
        )

    if os.stat(m.__file__).st_mtime < dir_entry.stat().st_mtime:
        raise RuntimeError(
            f"{os.path.basename(m.__file__)} is older than {mod}.pyx."
            " All .pyx files must be compiled before running the tests."
        )

    # For each callable in `mod` with name `test_*`,
    # set the result as an attribute of this module.
    for name in dir(m):
        value = getattr(m, name)
        if callable(value) and name.startswith("test_"):
            if name in test_names:
                raise RuntimeError(f"Duplicated test name: {name}")
            test_names.add(name)
            setattr(sys.modules[__name__], name, value)
