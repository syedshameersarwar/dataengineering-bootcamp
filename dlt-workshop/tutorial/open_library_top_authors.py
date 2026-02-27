# Top 10 Authors by Book Count — Open Library pipeline
# From dlt-workshop/tutorial: uv sync && uv run marimo edit open_library_top_authors.py

import marimo

__generated_with = "0.20.2"
app = marimo.App()


@app.cell
def imports():
    import marimo as mo
    import ibis
    import matplotlib.pyplot as plt
    from open_library_pipeline import pipeline

    return ibis, mo, pipeline, plt


@app.cell
def header(mo):
    mo.md(r"""
    ## Top 10 authors by book count
    Data from the Open Library pipeline (`books__authors`), using **ibis** for data access.
    """)
    return


@app.cell
def load_data(pipeline):
    dataset = pipeline.dataset()
    dataset_name = pipeline.dataset_name
    ibis_conn = dataset.ibis()
    authors_t = ibis_conn.table("books__authors", database=dataset_name)
    return (authors_t,)


@app.cell
def _(authors_t):
    authors_t
    return


@app.cell
def _():
    return


@app.cell
def query(authors_t, ibis):
    top_authors_expr = (
       authors_t
        .group_by(authors_t.name)
        .aggregate(book_count=authors_t.count())   # aggregated column is 'book_count'
        .order_by(ibis.desc("book_count"))        # use the correct name here
        .limit(10)
    )
    top_authors_df = top_authors_expr.to_pandas()
    return (top_authors_df,)


@app.cell
def chart(mo, plt, top_authors_df):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(
        top_authors_df["name"].iloc[::-1],
        top_authors_df["book_count"].iloc[::-1],
        color="steelblue",
        edgecolor="navy",
        alpha=0.8,
    )
    ax.set_xlabel("Number of books")
    ax.set_ylabel("Author")
    ax.set_title("Top 10 authors by book count (Open Library)")
    ax.invert_yaxis()
    plt.tight_layout()
    mo.vstack([mo.ui.table(top_authors_df), fig], gap=24)
    return


if __name__ == "__main__":
    app.run()
