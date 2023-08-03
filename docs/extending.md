# Extending Documentation

This page provides the basic info on where to start with writing documentation.
If you feel lost at any point, please check out the documentation of [MkDocs](https://www.mkdocs.org)
and [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/), with which this documentation is built.

## Project layout

    mkdocs.yml            # The configuration file.
    docs/
        index.md          # The documentation homepage.
        gen_ref_pages.py  # Script for generating the code reference.
        ...               # Other markdown pages, images and other files.

The `docs/` folder contains all source Markdown files for the documentation.

You can find all documentation settings in `mkdocs.yml`. See the `nav` section for mapping of the left navigation tab and the Markdown files.

## Local instance & commands

To see the changes made to the documentation page locally, a local instance of `mkdocs` is required.
You can install all the required packages using:

```shell
pip install -r requirements.doc.txt
```

After installing, you can use the following `mkdocs` commands:

* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.

## Text formatting and other features

As the entire documentation is written in Markdown, all base [Markdown syntax](https://www.markdownguide.org/basic-syntax/) is supported. This means headings, **bold text**, *italics*, `inline code`, tables and many more.

This set of options can be further extended, if you ever find the need. See the possibilities in the [Material theme reference](https://squidfunk.github.io/mkdocs-material/reference/).

???+ tip "Some of the enabled extensions"

    - This is an example of a collapsable admonition with a custom title.
    - Admonitions are one of the enabled markdown extensions, an another example would be the TODO checklist syntax:
        - [ ] Unchecked item
        - [x] Checked item
    - See the `markdown_extensions` section in `mkdocs.yml` for all enabled extensions.

### Links and references

To reference an anchor within a page, such as a heading, use a Markdown link to the specific anchor, for example: [Commands](#local-instance-commands).
If you're not sure which identifier to use, you can look at a heading's anchor by clicking the heading in your Web browser, either in the text itself, or in the table of contents.
If the URL is `https://example.com/some/page/#anchor-name` then you know that this item is possible to link to with `[<displayed text>](#anchor-name)`. (Tip taken from [mkdocstrings](https://mkdocstrings.github.io/usage/#finding-out-the-anchor))

To make a reference to another page within the documentation, use the path to the Markdown source file, followed by the desired anchor. For example, this [link](index.md#repository-structure) was created as `[link](index.md#repository-structure)`.

When making references to the generated [Code Reference](reference/index.md), there are two options. Links can be made either using the standard Markdown syntax, where some reverse-engineering of the generated files is required, or, with the support of mkdocstrings, using the `[example][full.path.to.object]` syntax. A real link like this can be for example [this one][dp3.common.config.ModelSpec] to the Platform Model Specification.

## Code reference generation

Code reference is generated using [mkdocstrings](https://mkdocstrings.github.io/) and the [Automatic code reference pages](https://mkdocstrings.github.io/recipes/#automatic-code-reference-pages) recipe from their documentation.
The generation of pages is done using the `docs/gen_ref_pages.py` script. The script is a slight modification of what is recommended within the mentioned recipe.

Mkdocstrings itself enables generating code documentation from its docstrings using a ```::: path.to.object``` syntax.
Here is an example of documentation for `dp3.snapshots.snapshot_hooks.SnapshotTimeseriesHookContainer.register` method:

### ::: dp3.snapshots.snapshot_hooks.SnapshotTimeseriesHookContainer.register
    options:
        show_root_heading: true
        show_root_full_path: false
        show_source: false
        show_signature_annotations: true

There are additional options that can be specified, which affect the way the documentation is presented. For more on these options, see [here](https://mkdocstrings.github.io/python/usage/#globallocal-options).

Even if you create a duplicate code reference description, the mkdocstring-style link still leads to the code reference, as you can see [here][dp3.snapshots.snapshot_hooks.SnapshotTimeseriesHookContainer.register].

## Deployment

The documentation is updated and deployed automatically with each push to selected branches thanks to the configured GitHub Action, which can be found in: `.github/workflows/deploy.yml`.
