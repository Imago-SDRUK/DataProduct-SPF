# SAD: How Does Cloudiness Matter?
## A Collaborative Literature Review

This repository contains a categorized bibliography exploring the relationship between sunlight exposure, cloudiness, and various outcomes including economic behavior, health, education, and political behavior.

## Repository Structure

```
.
├── main.tex          # Main LaTeX document
├── SAD.bib           # Bibliography database
└── README.md         # This file
```

## Adding New References

### Step 1: Add to SAD.bib

Open `SAD.bib` and add your new entry following the BibTeX format. Each entry should include:

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

### Step 3: Test Compilation

Before committing, ensure the document compiles:

```bash
pdflatex main.tex
biber main
pdflatex main.tex
pdflatex main.tex
```

Or if you're using a LaTeX editor, compile with the Biber backend enabled.

## Current Categories

The bibliography is organized into these sections:

1. **Economic Behavior** (`economics`)
2. **Educational Outcomes** (`education`)
3. **Political Behavior** (`politics`)
4. **Health** (`health`)
5. **Theoretical Frameworks** (`theory`)
6. **Datasets** (`data`)

Note: Climate and Environmental Patterns section is currently commented out in `main.tex`. If you find something interesting, remember to un-comment this sub-bibliography!

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

## Workflow

1. **Pull** the latest changes from the repository
2. **Add** your references to `SAD.bib`
3. **Assign** appropriate keywords
4. **Test** compilation locally
5. **Commit** with a descriptive message (e.g., "Add 3 papers on sunshine and voter turnout")
6. **Push** to the repository
