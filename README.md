# collegram

A Python package and associated scripts to collect, anonymise and preprocess Telegram
data.


## Collection flow

In very brief:
- get a first seed of channels by running `scripts/chan_keyword_search.py`
- perform a snowballing exploration of channels using the similar channels recommended
  by Telegram and the ones forwarded in already found ones, by running
  `scripts/channel_expansion.py`.

See diagrams in `reports/` for more info.


## Project Organization

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modelling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── scripts            <- Scripts to send to a cluster e.g.
    │
    ├── notebooks          <- Jupyter notebooks.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── environment.yml    <- The conda environment file for reproducing the analysis environment, e.g.
    │                         generated with `conda env export -f environment.yml`
    |
    ├── collegram          <- Source code for use in this project.
    |
    └── setup.py           <- makes project pip installable (pip install -e .) so collegram can be imported




--------

<p><small>Project based on <a target="_blank" href="https://github.com/drivendata/cookiecutter-data-science">a fork of the cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
