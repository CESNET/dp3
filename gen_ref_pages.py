"""Generate the code reference pages."""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

EXCLUDE_str = [
    ".core_modules",
    "template",
]

EXCLUDE = [f.format(p) for p in EXCLUDE_str for f in ["dp3/{}/*", "dp3/{}/**/*"]]

for path in sorted(Path("dp3").rglob("*.py")):
    if any(path.match(excluded) for excluded in EXCLUDE):
        continue

    if any(excluded in str(path) for excluded in EXCLUDE_str):
        continue

    module_path = path.with_suffix("")
    doc_path = path.relative_to("dp3").with_suffix(".md")
    full_doc_path = Path("reference", doc_path)
    parts = list(module_path.parts)

    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")
    elif parts[-1] == "__main__":
        continue

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(parts)
        print(f"# ::: {identifier}", file=fd)

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
