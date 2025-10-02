# SAD: How Does Cloudiness Matter?
## A Collaborative Reading List

![Build Status](https://github.com/itsshaonlee/spf/workflows/Build%20LaTeX%20document/badge.svg)

This repository contains an annotated bibliography organized by theme, exploring the relationship between sunlight exposure, cloudiness, and various outcomes including economic behavior, health, education, and political behavior.

**Latest PDF**: After each commit, a compiled PDF is automatically generated. See [Accessing the Compiled PDF](#accessing-the-compiled-pdf) below.

## Repository Structure

```
.
├── .github/
│   └── workflows/
│       └── compile-latex.yml    # Automated compilation
├── Literature/
│   ├── main.tex                 # Main LaTeX document
│   ├── SAD.bib                  # Bibliography database
│   └── .latexmkrc              # LaTeX configuration for biber
└── README.md                    # This file
```

## Accessing the Compiled PDF

Every time changes are pushed to the repository, GitHub automatically compiles a new PDF.

### To download the latest PDF:

1. Go to the **Actions** tab at the top of this repository
2. Click on the most recent successful workflow run (green checkmark ✓)
3. Scroll down to **Artifacts**
4. Download **SAD-Literature-Review.zip**
5. Extract the ZIP to get your PDF

The compilation typically takes 3-5 minutes.

## Adding New References

### Step 1: Add to SAD.bib

Navigate to `Literature/SAD.bib` and add your new entry following the BibTeX format. Each entry should include:

1. **Citation key**: Use format `authornameYEARkeyword` (e.g., `fleming2018valuing`)
2. **Required fields**: author, title, journal/booktitle, year
3. **Recommended fields**: volume, pages, doi, abstract
4. **Keywords field**: This is crucial for categorization (see below)

Example:
```bibtex
@article{yourname2024topic,
  title     = {Your Article Title},
  author    = {Last, First and Other, Author},
  journal   = {Journal Name},
  volume    = {10},
  pages     = {1--20},
  year      = {2024},
  doi       = {10.xxxx/xxxxx},
  abstract  = {Your abstract text here},
  keywords  = {economics}
}
```

### Step 2: Assign Keywords

Use **exactly one** of these keywords to categorize your entry:

- `economics` - Economic behavior, financial decisions, market impacts
- `health` - Health outcomes, vitamin D, physical/mental health
- `education` - Educational outcomes, learning, cognitive function
- `politics` - Political behavior, voting patterns
- `theory` - Theoretical frameworks and models
- `data` - Datasets, measurement methods, climate data

**Important**: The keyword field is case-sensitive and must match exactly.

### Step 3: Automatic Compilation

Once you commit your changes:
- GitHub Actions will automatically compile the LaTeX document
- Check the **Actions** tab to monitor progress
- If successful (green checkmark), download the PDF from the artifacts
- If failed (red X), click on the workflow run to see error logs

No need to compile locally unless you want to preview your changes!

## Current Categories

The bibliography is organized into these sections:

1. **Economic Behavior** (`economics`)
2. **Educational Outcomes** (`education`)
3. **Political Behavior** (`politics`)
4. **Health** (`health`)
5. **Theoretical Frameworks** (`theory`)
6. **Datasets** (`data`)

Note: Climate and Environmental Patterns section is currently commented out in `main.tex`.

## Adding a New Category

If you need to add a new thematic category:

1. Choose a keyword (e.g., `climate`)
2. Add the filter definition in `main.tex`:
   ```latex
   \defbibfilter{climate}{keyword={climate}}
   ```
3. Add the bibliography section:
   ```latex
   \printbibliography[title={Climate and Environmental Patterns}, heading=subbibliography, filter=climate]
   ```

## Formatting Guidelines

- **Abstracts**: Include abstracts when available - they appear automatically in the compiled PDF
- **DOIs**: Always include DOIs when available
- **URLs**: Include URLs for online resources
- **Special characters**: Use LaTeX commands (e.g., `{\'e}` for é)

## Compilation Requirements

This document requires:
- LaTeX distribution (TeX Live, MiKTeX, etc.)
- `biblatex` package with APA style
- `biber` backend (not BibTeX)
- Additional packages: geometry, csquotes, hyperref

## Workflow for Contributors

1. **Edit files**: Navigate to `Literature/SAD.bib` (or other files) and click the pencil icon (✏️)
2. **Make changes**: Add your references following the format guidelines
3. **Commit**: Scroll down, write a descriptive commit message (e.g., "Add 3 papers on sunshine and health outcomes")
4. **Wait for build**: Go to Actions tab and wait for the green checkmark
5. **Download PDF**: Get the compiled PDF from the artifacts section

## Troubleshooting

**If compilation fails:**
- Click on the failed workflow run (red X) in the Actions tab
- Expand the "Compile LaTeX document" step to see error logs
- Common issues:
  - Missing commas or braces in `.bib` entries
  - Special characters not properly escaped
  - Mismatched quotation marks
  - Invalid BibTeX syntax

**Getting help:**
- Open an issue in this repository
- Include the error message from the Actions log
- Mention which entry you added

## Local Compilation (Optional)

If you prefer to compile locally before pushing:

```bash
cd Literature
pdflatex main.tex
biber main
pdflatex main.tex
pdflatex main.tex
```

Requires: LaTeX distribution with biblatex, biber, and required packages.

