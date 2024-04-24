import streamlit as st
from streamlit_mic_recorder import speech_to_text
import snowflake.connector
import openai
import pandas as pd
import base64
 
openai.api_key = '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'
 
# Initialize session state variables
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "selected_database" not in st.session_state:
    st.session_state.selected_database = None
if "selected_schema" not in st.session_state:
    st.session_state.selected_schema = None
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None
if "query_execution_failed" not in st.session_state:
    st.session_state.query_execution_failed = False
if "transcribed_text" not in st.session_state:
    st.session_state.transcribed_text = ""
if "generated_sql_query" not in st.session_state:    
    st.session_state.generated_sql_query = ""
 
def refresh():
    st.session_state.selected_database = None
    st.session_state.selected_schema = None
    st.session_state.selected_table = None
    st.session_state.query_execution_failed = False
    st.session_state.transcribed_text = ""
    st.rerun()
 
def login_page():
    st.subheader(":blue[Snowflake Login]")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    account = st.text_input("Account Name")
    if st.button("Login"):
        conn = connect_to_snowflake(username, password, account)
        if conn:
            st.session_state.logged_in = True
            st.session_state.conn = conn
            return True
    return False
 
def get_base64_of_bin_file(png_file):
    with open(png_file, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()
 
def build_markup_for_logo(png_file, image_width="120px"):
    binary_string = get_base64_of_bin_file(png_file)
    return f"""
            <img src="data:image/png;base64,{binary_string}" width="{image_width}">
            """
 
def add_logo(png_file):
    logo_markup = build_markup_for_logo(png_file)
    st.markdown(logo_markup, unsafe_allow_html=True)
 
st.markdown("""
    <style>
        .block-container {
            padding-top: 0px;
        }
    </style>
    """, unsafe_allow_html=True)
 
st.write(" ")
st.write(" ")
add_logo("snow_mic2.png")
 
st.title("VoiceQuery SnowHub")
 
def connect_to_snowflake(username, password, account):
    try:
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None
 
def list_databases(conn):
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    return [row[1] for row in cursor.fetchall()]
 
def list_schemas(conn, database):
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {database}")
    cursor.execute("SHOW SCHEMAS")
    return [row[1] for row in cursor.fetchall()]
 
def list_tables(conn, database, schema):
    cursor = conn.cursor()
    cursor.execute(f"USE DATABASE {database}")
    cursor.execute(f"USE SCHEMA {schema}")
    cursor.execute("SHOW TABLES")
    return [row[1] for row in cursor.fetchall()]
 
def fetch_and_display_table_data(conn, database, schema, table):
    try:
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        data = rows
        df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
        column = df.columns.tolist()
        col = ', '.join(column)
        return col
    except Exception as e:
        st.error(f"Error fetching data from table: {e}")
 
def voice_to_text_page(conn, column):
    col1 , col2,col3 , col4,col5 , col6,col7 , col8 = st.columns(8)
    with col1:
        st.write(" ")
        st.write(" ")
        text = speech_to_text(
            language='en',
            start_prompt="Click to record",
            stop_prompt="Drop the Mic",
            just_once=False,
            use_container_width=False,
            callback=None,
            args=(),
            kwargs={},
            key=None
        )
        with col2:
            add_logo("mic2.png")
    if text:
        st.success('Audio transcription completed.')
        st.session_state.transcribed_text = text
        text2 = text # Store the transcribed text in session state
        text = column + text
        st.write(f"Transcribed text: {text2}")
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful SQL assistant."},
                    {"role": "user", "content": text}
                ],
                max_tokens=1000,
                n=1,
                stop=None,
                temperature=0.5,
            )
            model_output = response['choices'][0]['message']['content'].strip()
            sql_index = model_output.find("sql")
            if sql_index != -1:
                sql_index += 4
                sql_query = model_output[sql_index:].split(';')[0].upper()
                st.write(f"Extracted SQL query: {sql_query}")
                st.session_state.generated_sql_query = sql_query
                execute_sql_query(conn, sql_query)
                #return sql_query
        except Exception as e:
            st.write(f"Error: {e}")
 
def execute_sql_query(conn, sql_query):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        if sql_query.strip().upper().startswith("SELECT"):
            rows = cursor.fetchall()
            st.write("Query Result:")
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            st.dataframe(df)
        else:
            st.success("SQL query executed successfully.")
        return True
    except Exception as e:
        st.error(f"Error executing SQL query: {e}")
        st.session_state.query_execution_failed = True
        return False

 
def main():
    
    if st.session_state.logged_in:
        
        if st.button("Refresh"):
            refresh()
        database_schema_table_page(st.session_state.conn)
    else:
        login_page()
 
def database_schema_table_page(conn):
    st.sidebar.title("Databases")
    databases = list_databases(conn)
    selected_database = st.sidebar.selectbox("Select a Database", databases, on_change=refresh)
    st.session_state.selected_database = selected_database
 
    if selected_database:
        st.sidebar.title("Schemas")
        schemas = list_schemas(conn, selected_database)
        selected_schema = st.sidebar.selectbox("Select a Schema", schemas, on_change=refresh)
        st.session_state.selected_schema = selected_schema
 
        if selected_schema:
            st.sidebar.title("Tables")
            tables = list_tables(conn, selected_database, selected_schema)
            selected_table = st.sidebar.selectbox("Select a Table", tables, on_change=refresh)
            st.session_state.selected_table = selected_table
 
            if selected_table:
                colu = fetch_and_display_table_data(conn, selected_database, selected_schema, selected_table)
                querry = voice_to_text_page(conn, colu)
            else:
                voice_to_text_page(conn, "")

            execute = st.checkbox("Execute")
            if execute:
                st.subheader("Manual Query Execution")
                if "generated_sql_query" in st.session_state and st.session_state.generated_sql_query:                    # Populate the text box with the generated SQL query                   
                    query = st.text_input("Enter the sql query", value=st.session_state.generated_sql_query)

                if st.button("Execute"):
                    execute_sql_query(conn, query)
 
if __name__ == "__main__":
    main()
 
