import os
import shutil
from io import StringIO

import duckdb
import streamlit as st
from code_editor import code_editor

st.set_page_config(page_title="ducklit", page_icon=":duck:")


# @st.cache_resource
def get_db_connection():
    if "duck_conn" not in st.session_state:
        st.session_state["duck_conn"] = duckdb.connect(":memory:")

    return st.session_state["duck_conn"]


def main():
    conn = get_db_connection()
    create_side_bar(conn)
    create_page(conn)


def create_side_bar(conn: duckdb.DuckDBPyConnection):
    cur = conn.cursor()

    with st.sidebar:
        st.button("load sample data", on_click=load_sample_data, args=[conn])
        files = st.file_uploader(
            "select one or more CSV or JSON files", accept_multiple_files=True
        )
        load_files(conn, files)

        st.divider()

        table_list = ""
        cur.execute("show all tables")
        recs = cur.fetchall()

        if len(recs) > 0:
            st.markdown("# tables")

        for rec in recs:
            table_name = rec[2]
            table_list += f"- {table_name}\n"
            cur.execute(f"describe {table_name}")

            for col in cur.fetchall():
                table_list += f"    - {col[0]} {col[1]}\n"

        st.markdown(table_list)


def load_files(conn: duckdb.DuckDBPyConnection, files: list):
    for file in files:
        stringio = StringIO(file.getvalue().decode("utf-8"))

        if file.name.endswith(".csv"):
            conn.read_csv(stringio).to_table(file.name[:-4])
        elif file.name.endswith(".json"):
            with open(file.name, "w") as temp_file:
                stringio.seek(0)
                shutil.copyfileobj(stringio, temp_file)

            conn.read_json(file.name).to_table(file.name[:-5])
            os.remove(file.name)


def load_sample_data(conn: duckdb.DuckDBPyConnection):
    conn.read_json("sample_data/posts.json").to_table("posts")


def create_page(conn: duckdb.DuckDBPyConnection):
    st.title("ducklit :duck:")
    st.write("Query your files with DuckDB")
    st.divider()

    cur = conn.cursor()
    st.write(
        "hint: you can write multiple queries as long as each one ends with a semicolon"
    )
    st.write("ctrl+enter to run the SQL")
    res = code_editor(code="", lang="sql", key="editor")

    for query in res["text"].split(";"):
        if query.strip() == "":
            continue

        try:
            cur.execute(query)
            df = cur.fetch_df()
            st.write(df)
        except Exception as e:
            st.error(e)

    if st.button("reset database"):
        st.cache_resource.clear()
        st.session_state["editor"]["text"] = ""
        st.rerun()


if __name__ == "__main__":
    main()
